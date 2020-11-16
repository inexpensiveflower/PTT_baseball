import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
from datetime import datetime

host = 'localhost'
dbname = 'PTT_baseball'

# 建立連線
client = MongoClient(host, 27017)
print("資料庫連線成功!")

# 選定資料庫
db = client[dbname]

# 選定資料表
article_collection = db.article
response_collection = db.response


start_index = 10106
url = "https://www.ptt.cc/bbs/Baseball/"
pages = 1

def get_post_url(url):
	for i in range(0, pages):
		# 網頁網址
		target_url = url + 'index' + str(start_index - i) + '.html'
		# print('網頁連結是 : ', target_url)

		web = requests.get(target_url)
		soup = BeautifulSoup(web.text, 'lxml')
		
		# 檢查有沒有文章，沒有就不跑了
		if soup.find('div', class_ = 'r-ent'):
			post_list = soup.find_all("div", class_ = "title")
			
			# 一篇文章一篇文章送去 get_post_info 做爬蟲
			for post in post_list:
				post_url = post.find("a").get('href')
				# print("文章連結 : ", post_url)
				complete_url = "https://www.ptt.cc" + post_url
				get_post_info(complete_url)


def get_post_info(url):
	# dictionary 的 key 是文章連結，value 是文章的資訊
	post_info_dict = dict()
	# 
	author_title_time_push = dict()
	web = requests.get(url)
	soup = BeautifulSoup(web.text, 'lxml')
	
	post_info = soup.find_all("div", class_ = "article-metaline")

	author_title_time_push['author'] = post_info[0].find("span", class_ = "article-meta-value").get_text(strip = True)
	author_title_time_push['title'] = post_info[1].find("span", class_ = "article-meta-value").get_text(strip = True)
	try:
		author_title_time_push['post_time'] = datetime.strptime(post_info[2].find("span", class_ = "article-meta-value").get_text(strip = True), '%a %b %d %H:%M:%S %Y') 
	except:
		pass
	
			
	# 抓出全部留言，並宣告算噓推序的變數	
	reply_list = soup.find_all("div", class_ = "push")
	print("留言數 : ", len(reply_list))
	score = 0
	push_count = 0
	abstract_count = 0

	# 計算噓推數量的迴圈
	for reply in reply_list:
		push_tag = reply.find('span', class_ = 'push-tag').get_text(strip = True)
		if "推" in push_tag:
			push_count += 1
		elif "噓" in push_tag:
			abstract_count += 1
		else:
			pass

	score = score + push_count - abstract_count

	author_title_time_push['good_push'] = push_count
	author_title_time_push['bad_push'] = abstract_count
	author_title_time_push['post_score'] = score

	post_info_dict[url] = author_title_time_push

	key = list(post_info_dict.keys())[0]
	value = list(post_info_dict.values())[0]

	post_id = insert_post(key, value)
	# get_post_replies(post_id, soup)


def insert_post(post_url, post_info):

	doc = article_collection.find_one({'post_url':post_url})
	post_info['update_time'] = datetime.now()
	post_info['post_url'] = post_url

	if not doc:
		post_id = article_collection.insert_one(post_info).inserted_id
	else:
		article_collection.update_one(
			{'_id':doc['_id']},
			{'$set':post_info}
		)
		post_id = doc['_id']

	print(post_info['title'], "   新增成功!")
	time.sleep(1)

	return post_id

def get_post_replies(post_id, soup):

	reply_list = soup.find_all("div", class_ = "push")

	results = []

	# print(reply_list)

	if reply_list:
		for reply in reply_list:

			# print("回覆~~~" ,reply)
		
			result = {'article_id':post_id}

			result['push'] = reply.find('span', class_ = 'push-tag').get_text(strip = True)
			result['reply_id'] = reply.find('span', class_ = 'push-userid').get_text(strip = True)
			result['reply_content'] = reply.find('span', class_ = 'push-content').get_text(strip = True)
			result['reply_replyTime'] = datetime.strptime(reply.find('span', class_ = 'push-ipdatetime').get_text(strip = True), '%m/%d %H:%M')

			results.append(result)
			# list 裡面每個元件都是一個 dictionary
			# print("一篇文的全部留言~~~~", results)

		# 傳一個 list 過去 insert_responses()
		# insert_responses(results)

def insert_responses(responses_list):


	doc = response_collection.find_one({'article_id':responses_list[0]['article_id']})

	if not doc:
		for response_dict in responses_list:
			reply_id = response_collection.insert_one(response_dict).inserted_id
	else:
		for response_dict in responses_list:
			response_collection.update_many(
				{'$set': response_dict},
				upsert=True)


if __name__ == "__main__":
	get_post_url(url)


