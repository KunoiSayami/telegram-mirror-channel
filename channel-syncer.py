#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# channel-syncer.py
# Copyright (C) 2019-2020 KunoiSayami
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
from typing import List, Union

import aiosqlite
import pyrogram
from pyrogram import Client, filters
from pyrogram.errors import RPCError
from pyrogram.handlers import MessageHandler, RawUpdateHandler
from pyrogram.types import Message, Update


class SqliteObject:
	def __init__(self, client: Client, config: ConfigParser, file_name):
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

	async def do_sth(self, action: str, msg: Union[Message, List[int]]) -> None:
		try:
			if action == 'normal':
				await self.handle_incoming_message(self.client, msg)
			elif action == 'edit':
				await self.handle_edit_message(self.client, msg)
			elif action == 'del':
				await self.delete_message(msg)
		except:
			self.logger.exception('Caught exception')

	async def request_target_message_id(self, message_ids: List[int], db: aiosqlite.Connection) -> List[int]:
		r = []
		for x in message_ids:
			try:
				r.append(await self.query_target_id(x, db))
			except TypeError:
				self.logger.exception(f'message_id: {x} not found')
		return r

	async def delete_message(self, msgs: List[int]) -> None:
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

	@staticmethod
	async def query_target_id(origin_id: int, db: aiosqlite.Connection):
		async with db.execute("SELECT `target_id` FROM `id_mapping` WHERE `origin_id` = ?", (origin_id,)) as cursor:
			return (await cursor.fetchone())[0]

	async def handle_incoming_message(self, client: Client, msg: Message) -> None:
		if msg.sticker and msg.sticker.file_id == self.split_sticker:
			r_msg = await client.send_sticker(self.fwd_to, msg.sticker.file_id)
		elif msg.forward_from or msg.forward_sender_name or msg.forward_from_chat:
			r_msg = await msg.forward(self.fwd_to)
		else:
			await self.handle_comment(client, msg)
			return
		await self.insert_into_db(msg, r_msg)

	@staticmethod
	def get_caption(msg: Message) -> str:
		return msg.caption if msg.caption is not None else ""

	async def handle_comment(self, client: Client, msg: Message) -> None:
		if msg.reply_to_message and not msg.reply_to_message.empty:
			r_id = await self.query_target_id(msg.reply_to_message.message_id)
		else:
			r_id = None
		msg_type = self.get_msg_type(msg)
		if msg_type == 'error':
			return
		if msg_type == 'text':
			r_msg = await client.send_message(self.fwd_to, msg.text, reply_to_message_id=r_id,
											  disable_web_page_preview=not msg.web_page)
		else:
			r_msg = await client.send_cached_media(self.fwd_to, getattr(msg, msg_type).file_id,
												   getattr(msg, msg_type).file_ref, self.get_caption(msg),
												   reply_to_message_id=r_id)
		await self.insert_into_db(msg, r_msg)

	async def handle_edit_message(self, client: Client, msg: Message) -> None:
		async with self.execute_lock, aiosqlite.connect(self.file_name) as db:
			target_id = await self.query_target_id(msg.message_id, db)
		text = msg.text if msg.text else msg.caption
		(client.edit_message_text if self.get_msg_type(msg) == 'text' else client.edit_message_caption)(self.fwd_to,
																										target_id, text)

	@staticmethod
	def get_msg_type(msg: Message) -> str:
		return 'photo' if msg.photo else \
			'video' if msg.video else \
			'animation' if msg.animation else \
			'document' if msg.document else \
			'text' if msg.text else \
			'voice' if msg.voice else 'error'


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
		self.q = SqliteObject(self.app, config)

		self.app.add_handler(MessageHandler(self.handle_edit_message, filters.chat(self.listen_group) & filters.edited))
		self.app.add_handler(MessageHandler(self.handle_incoming_message, filters.chat(self.listen_group)))
		self.app.add_handler(RawUpdateHandler(self.handle_raw_update))

	async def start(self):
		await self.app.start()

	async def stop(self):
		await self.app.stop()

	async def handle_raw_update(self, _client: Client, update: Update, _chats: dict, _users: dict):
		if isinstance(update, pyrogram.raw.types.UpdateDeleteChannelMessages) and \
				(-(update.channel_id + 1000000000000) == self.listen_group):
			await self.q.do_sth('del', update.messages)

	async def handle_edit_message(self, _, msg: Message) -> None:
		await self.q.do_sth('edit', msg)

	async def handle_incoming_message(self, _, msg: Message) -> None:
		await self.q.do_sth('normal', msg)


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
