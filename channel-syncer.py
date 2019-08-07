# -*- coding: utf-8 -*-
import sqlite3
from configparser import ConfigParser
from threading import Lock, Thread
import logging
import time
from queue import Queue
import pyrogram
from pyrogram import Client, MessageHandler, Message, Filters, RawUpdateHandler, Update

class sqlite_object(Thread):
	def __init__(self, client: Client, config: ConfigParser):
		Thread.__init__(self, daemon=True)
		self.db_conn = None
		self.conn = None
		self.config = config
		self.client = client
		self.logger = logging.getLogger(__name__)
		self.logger.setLevel(logging.DEBUG)
		self.execute_lock = Lock()
		self.queue = Queue()
		self.listen_group = int(self.config['channel']['listen_group'])
		self.fwd_to = int(self.config['channel']['fwd_to'])
		self.split_sticker = self.config['channel']['split_sticker']

	def run(self):
		self.db_conn = sqlite3.connect('channel-no.db')
		self.conn = self.db_conn.cursor()
		while True:
			while self.queue.empty():
				time.sleep(0.1)
			while not self.queue.empty():
				r = self.queue.get_nowait()
				self.do_sth(*r)

	def do_sth(self, action: str, msg: Message):
		try:
			if action == 'normal':
				self.handle_incoming_message(self.client, msg)
			elif action == 'edit':
				self.handle_edit_message(self.client, msg)
			elif action == 'del':
				self.delete_message(msg)
		except:
			self.logger.exception('Catched exception')

	def request_target_message_id(self, message_ids: list):
		r = []
		for x in message_ids:
			try:
				r.append(self.query_target_id(x))
			except TypeError:
				print(f'message_id: {x} not found')
		return r

	def delete_message(self, msgs: list):
		try:
			target_ids = self.request_target_message_id(msgs)
			if len(target_ids) == 0:
				return self.client.send_message(self.listen_group, '[WARNING] delete message failed.', disable_notification=True)
			self.client.delete_messages(self.fwd_to, target_ids)
		except pyrogram.errors.RPCError:
			self.client.send_message('Got rpc error, see console to get more information')

	def query1(self, sql: str, args: tuple = ()):
		return self.execute(sql, args).fetchone()

	def execute(self, sql: str, args: tuple = ()):
		with self.execute_lock:
			return self.conn.execute(sql, args)

	def commit(self):
		with self.execute_lock:
			self.db_conn.commit()
			self.conn.close()
			self.conn = self.db_conn.cursor()

	def close(self):
		with self.execute_lock:
			self.db_conn.commit()
			self.conn.close()
			self.db_conn.close()

	def insert_into_db(self, msg: Message, r_msg: Message):
		if r_msg is None: return
		self.execute("INSERT INTO `id_mapping` VALUES (?, ?)", (msg.message_id, r_msg.message_id))

	def query_target_id(self, origin_id: int):
		return self.query1("SELECT `target_id` FROM `id_mapping` WHERE `origin_id` = %d"%origin_id)[0]

	def handle_incoming_message(self, client: Client, msg: Message):
		#self.logger.debug('msg => %s', repr(msg))
		r_msg = None
		if msg.sticker and msg.sticker.file_id == self.split_sticker:
			r_msg = client.send_sticker(self.fwd_to, msg.sticker.file_id)
		elif msg.forward_from or msg.forward_sender_name or msg.forward_from_chat:
			r_msg = msg.forward(self.fwd_to)
		else:
			return self.handle_comment(client, msg)
		self.insert_into_db(msg, r_msg)
		self.commit()

	def handle_comment(self, client: Client, msg: Message):
		r_msg = None
		if msg.reply_to_message and not msg.reply_to_message.empty:
			r_id = self.query_target_id(msg.reply_to_message.message_id)
		else:
			r_id = None
		msg_type = self.get_msg_type(msg)
		if msg_type == 'error':
			return
		if msg_type == 'text':
			r_msg = client.send_message(self.fwd_to, msg.text, reply_to_message_id=r_id, disable_web_page_preview=not msg.web_page)
		else:
			r_msg = client.send_cached_media(self.fwd_to, getattr(msg, msg_type).file_id, msg.caption, reply_to_message_id=r_id)
		self.insert_into_db(msg, r_msg)
		self.commit()

	def handle_edit_message(self, client: Client, msg: Message):
		target_id = self.query_target_id(msg.message_id)
		text = msg.text if msg.text else msg.caption
		(client.edit_message_text if self.get_msg_type(msg) == 'text' else client.edit_message_caption)(self.fwd_to, target_id, text)

	def handle_delete_message(self, client: Client, msg: Message):
		pass

	@staticmethod
	def get_msg_type(msg: Message):
		return 'photo' if msg.photo else \
			'video' if msg.video else \
			'animation' if msg.animation else \
			'document' if msg.document else \
			'text' if msg.text else \
			'voice' if msg.voice else 'error'

class forward_thread:
	def __init__(self):
		self.config = ConfigParser()
		self.config.read('config.ini')

		self.listen_group = int(self.config['channel']['listen_group'])
		self.fwd_to = int(self.config['channel']['fwd_to'])

		self.app =  Client(
			'channel_user',
			self.config['account']['api_id'],
			self.config['account']['api_hash'],
			#bot_token = self.config['channel']['channel_bot_token'],
			#no_updates=True
		)
		self.q = sqlite_object(self.app, self.config)

		#self.app.add_handler(MessageHandler(self.handle_incoming_message, Filters.chat(self.listen_group) & Filters.command('del')))
		self.app.add_handler(MessageHandler(self.handle_edit_message, Filters.chat(self.listen_group) & Filters.edited))
		self.app.add_handler(MessageHandler(self.handle_incoming_message, Filters.chat(self.listen_group)))
		self.app.add_handler(RawUpdateHandler(self.handle_raw_update))

	def start(self):
		self.q.start()
		self.app.start()

	def idle(self):
		self.app.idle()

	def handle_raw_update(self, _client: Client, update: Update, _chats: dict, _users: dict):
		if isinstance(update, pyrogram.api.types.UpdateDeleteChannelMessages) and \
			(-(update.channel_id + 1000000000000) == self.listen_group):
			self.q.queue.put_nowait(('del', update.messages))

	def handle_edit_message(self, _, msg: Message):
		self.q.queue.put_nowait(('edit', msg))

	def handle_incoming_message(self, _, msg: Message):
		self.q.queue.put_nowait(('normal', msg))


if __name__ == "__main__":
	logging.getLogger("pyrogram").setLevel(logging.WARNING)
	logging.basicConfig(level=logging.DEBUG, format = '%(asctime)s - %(levelname)s - %(name)s - %(funcName)s - %(lineno)d - %(message)s')
	f = forward_thread()
	f.start()
	#f.spider()
	f.idle()