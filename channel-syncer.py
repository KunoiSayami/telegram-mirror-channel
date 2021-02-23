#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# channel-syncer.py
# Copyright (C) 2019-2021 KunoiSayami
#
# This module is part of telegram-mirror-channel and is released under
# the AGPL v3 License: https://www.gnu.org/licenses/agpl-3.0.txt
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
import asyncio
import logging
from configparser import ConfigParser
from concurrent.futures import Future
from typing import Iterator, Optional

import aiosqlite
import pyrogram
from pyrogram import Client, filters
from pyrogram.errors import RPCError
from pyrogram.handlers import MessageHandler, RawUpdateHandler
from pyrogram.types import Message, Update


class SqliteObject:
    def __init__(self, client: Client, config: ConfigParser, file_name: str):
        self.db_conn = None
        self.conn = None
        self.client = client
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.execute_lock = asyncio.Lock()
        self.listen_group = config.getint('channel', 'listen_group')
        self.fwd_to = config.getint('channel', 'fwd_to')
        self.split_sticker = config.get('channel', 'split_sticker')
        self.file_name: str = file_name

    async def request_target_message_id(self, message_ids: list[int], db: aiosqlite.Connection) -> list[int]:
        r = []
        for x in message_ids:
            try:
                r.append(await self.query_target_id(x, db))
            except TypeError:
                self.logger.exception(f'message_id: {x} not found')
        return r

    async def delete_message(self, msgs: list[int]) -> None:
        try:
            async with self.execute_lock, aiosqlite.connect(self.file_name) as db:
                target_ids = await self.request_target_message_id(msgs, db)
            if not target_ids:
                await self.client.send_message(self.listen_group, '[WARNING] delete message failed.',
                                               disable_notification=True)
                return
            await self.client.delete_messages(self.fwd_to, target_ids)
        except RPCError:
            await self.client.send_message(self.listen_group, 'Got rpc error, see console to get more information')

    async def insert_into_db(self, msg: Message, r_msg: Message):
        if r_msg is None:
            return
        async with self.execute_lock, aiosqlite.connect(self.file_name) as db:
            async with db.execute("INSERT INTO `id_mapping` VALUES (?, ?)", (msg.message_id, r_msg.message_id)):
                pass
            await db.commit()

    async def insert_many(self, data: Iterator[tuple[int, int]]) -> None:
        async with self.execute_lock, aiosqlite.connect(self.file_name) as db:
            async with db.executemany("INSERT INTO `id_mapping` VALUES (?, ?)", data):
                pass
            await db.commit()

    @staticmethod
    async def query_target_id(origin_id: int, db: aiosqlite.Connection):
        async with db.execute("SELECT `target_id` FROM `id_mapping` WHERE `origin_id` = ?", (origin_id,)) as cursor:
            return (await cursor.fetchone())[0]

    @staticmethod
    def get_caption(msg: Message) -> str:
        return msg.caption if msg.caption is not None else ""

    async def handle_comment(self, client: Client, msg: Message) -> None:
        if msg.reply_to_message and not msg.reply_to_message.empty:
            async with self.execute_lock, aiosqlite.connect(self.file_name) as db:
                r_id = await self.query_target_id(msg.reply_to_message.message_id, db)
        else:
            r_id = None
        msg_type = self.get_msg_type(msg)
        if msg_type == 'error':
            return
        if msg_type == 'text':
            r_msg = await client.send_message(self.fwd_to, msg.text, reply_to_message_id=r_id,
                                              disable_web_page_preview=not msg.web_page)
        else:
            r_msg = await client.send_cached_media(self.fwd_to, getattr(msg, msg_type).file_id, self.get_caption(msg),
                                                   reply_to_message_id=r_id)
        await self.insert_into_db(msg, r_msg)

    async def handle_edit_message(self, client: Client, msg: Message) -> None:
        async with self.execute_lock, aiosqlite.connect(self.file_name) as db:
            target_id = await self.query_target_id(msg.message_id, db)
        text = msg.text if msg.text else msg.caption
        await (client.edit_message_text if self.get_msg_type(msg) == 'text' else client.edit_message_caption
               )(self.fwd_to, target_id, text)

    @staticmethod
    def get_msg_type(msg: Message) -> str:
        return 'photo' if msg.photo else \
            'video' if msg.video else \
            'animation' if msg.animation else \
            'document' if msg.document else \
            'text' if msg.text else \
            'voice' if msg.voice else 'error'


class Watcher:
    def __init__(self, client: Client, forward_to: int, sqlite: SqliteObject):
        self.client = client
        self.stop_event = asyncio.Event()
        self.queue = asyncio.Queue()
        self.sync_lock = asyncio.Lock()
        self.forward_to = forward_to
        self.sqlite = sqlite
        self.coroutine: Optional[Future] = None

    def set_stop(self) -> None:
        self.stop_event.set()

    async def put(self, msg: Message) -> None:
        async with self.sync_lock:
            self.queue.put_nowait(msg)

    async def monitor(self) -> None:
        while not self.stop_event.is_set():
            if not self.queue.empty():
                await self.forward_messages()
            await asyncio.sleep(1.5)
        self.coroutine = None

    def start(self) -> None:
        if self.coroutine is not None:
            return
        self.sqlite.logger.info('Start watcher')
        self.coroutine = asyncio.run_coroutine_threadsafe(self.monitor(), asyncio.get_event_loop())

    def wait(self, timeout: float = 1.) -> None:
        if self.coroutine is None:
            return
        self.coroutine.result(timeout)

    async def forward_messages(self) -> None:
        async with self.sync_lock:
            original_msg, forwarded_msg, pending_forward = [], [], []
            while not self.queue.empty():
                msg = self.queue.get_nowait()
                if msg.sticker and msg.sticker.file_unique_id == self.sqlite.split_sticker:
                    r_msg = await self.client.send_sticker(self.forward_to, msg.sticker.file_id,
                                                           disable_notification=True)
                    original_msg.append(msg.message_id)
                    forwarded_msg.append(r_msg.message_id)
                elif msg.forward_from or msg.forward_sender_name or msg.forward_from_chat:
                    pending_forward.append(msg)
                else:
                    if pending_forward:
                        msg_ids = [x.message_id for x in pending_forward]
                        fmsgs = await self.client.forward_messages(self.forward_to, pending_forward[0].chat.id, msg_ids)
                        if len(msg_ids) != len(fmsgs):
                            self.sqlite.logger.warning('Forward message length not equal! (except %d but %d found)',
                                                       len(msg_ids), len(fmsgs))
                        original_msg.extend([msg.message_id for msg in fmsgs])
                        await self.sqlite.insert_many(zip(original_msg, forwarded_msg))
                        original_msg, forwarded_msg, pending_forward = [], [], []
                    await self.sqlite.handle_comment(self.client, msg)
            if original_msg and forwarded_msg:
                await self.sqlite.insert_many(zip(original_msg, forwarded_msg))


class ForwardController:
    def __init__(self):
        config = ConfigParser()
        config.read('config.ini')

        self.listen_group: int = config.getint('channel', 'listen_group')
        self.fwd_to: int = config.getint('channel', 'fwd_to')

        self.app = Client(
            'channel_user',
            config.get('account', 'api_id'),
            config.get('account', 'api_hash'),
        )
        self.q = SqliteObject(self.app, config, config.get('database', 'file', fallback='channel-no.db'))
        self.watcher = Watcher(self.app, self.fwd_to, self.q)

        self.app.add_handler(MessageHandler(self.handle_toggle, filters.chat(self.listen_group) &
                                            filters.command('toggle')))
        self.app.add_handler(MessageHandler(self.handle_edit_message, filters.chat(self.listen_group) & filters.edited))
        self.app.add_handler(MessageHandler(self.handle_incoming_message, filters.chat(self.listen_group)))
        self.app.add_handler(RawUpdateHandler(self.handle_raw_update))
        self.enabled = True

    async def start(self):
        self.watcher.start()
        await self.app.start()

    async def stop(self):
        self.watcher.set_stop()
        await self.app.stop()

    async def handle_raw_update(self, _client: Client, update: Update, _chats: dict, _users: dict):
        if isinstance(update, pyrogram.raw.types.UpdateDeleteChannelMessages) and \
                (-(update.channel_id + 1000000000000) == self.listen_group):
            await self.q.delete_message(update.messages)

    async def handle_edit_message(self, client: Client, msg: Message) -> None:
        await self.q.handle_edit_message(client, msg)

    async def handle_toggle(self, _client: Client, msg: Message) -> None:
        self.enabled = not self.enabled
        await msg.reply(f'Set status to: {self.enabled}')

    async def handle_incoming_message(self, _, msg: Message) -> None:
        if not self.enabled:
            return
        await self.watcher.put(msg)


async def main() -> None:
    f = ForwardController()
    await f.start()
    await pyrogram.idle()
    await f.stop()


if __name__ == "__main__":
    logging.getLogger("pyrogram").setLevel(logging.WARNING)
    logging.getLogger('aiosqlite').setLevel(logging.WARNING)
    try:
        import coloredlogs

        coloredlogs.install(logging.DEBUG,
                            fmt='%(asctime)s,%(msecs)03d - %(levelname)s - %(funcName)s - %(lineno)d - %(message)s')
    except ModuleNotFoundError:
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(levelname)s - %(funcName)s - %(lineno)d - %(message)s')
    asyncio.get_event_loop().run_until_complete(main())
