import telegram
bot = telegram.Bot("1026028365:AAF-_SQ9y0rOGJkm6Kq0_i2M97V7pqdOYMA")
#chat_ids = [update.message.chat_id for update in bot.get_updates()]
def send_message(msg):
    bot.send_message(857336162, msg)

#send_message("hi")
#for id in chat_ids:
#    bot.send_message(id, "server_die")
