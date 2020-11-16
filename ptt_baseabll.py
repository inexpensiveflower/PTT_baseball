import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
from datetime import datetime
import re

host = 'localhost'
dbname = 'PTT_baseball'

# 建立連線
client = MongoClient(host, 27017)
print("資料庫連線成功!")

# 選定資料庫
db = client[dbname]

# 選定資料表
article_collection = db.article_1
response_collection = db.response_1

url = "https://www.ptt.cc/bbs/Baseball/"
pages = 2

def get_post_url(target_url):

	# 先抓首頁的連結最後面的 index 
	html_doc = requests.get(target_url)
	home_url = html_doc.url

	init_soup = BeautifulSoup(html_doc.text, 'lxml')

	# 直接抓首頁連結是沒有數字 index 的，所以要透過上一頁的連結來抓取數字 index
	right_up_corner = init_soup.find_all('a', class_ = 'btn wide')
	for url in right_up_corner:
		# print(url.get_text(strip = True))
		if url.get_text(strip = True) == "‹ 上頁":
			# get the home page's path
			home_url = url.get("href")
	
	# 因為抓到的數字是上一頁的，所以要加1加回來
	start_index = int(re.search('(\d+).*', home_url).group(1)) + 1

	PTT_baseball_domain = "https://www.ptt.cc/bbs/Baseball/"
	for i in range(0, pages):
		
		target_url = PTT_baseball_domain + 'index' + str(start_index - i) + '.html'
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

	# print(post_info)

	# 有些文章沒有作者、標題、日期的，就跳過不要跑
	try:
		author_title_time_push['author'] = post_info[0].find("span", class_ = "article-meta-value").get_text(strip = True)
	except:
		return(0)
	try:
		author_title_time_push['title'] = post_info[1].find("span", class_ = "article-meta-value").get_text(strip = True)
	except:
		return(0)
	try:
		author_title_time_push['post_time'] = datetime.strptime(post_info[2].find("span", class_ = "article-meta-value").get_text(strip = True), '%a %b %d %H:%M:%S %Y') 
	except:
		return(0)
	
			
	# 抓出全部留言，並宣告算噓推序的變數	
	reply_list = soup.find_all("div", class_ = "push")
	# print("留言數 : ", len(reply_list))
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
	get_post_replies(post_id, soup)


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

	if reply_list:
		for reply in reply_list:
		
			result = {'article_id':post_id}

			result['push'] = reply.find('span', class_ = 'push-tag').get_text(strip = True)
			result['reply_id'] = reply.find('span', class_ = 'push-userid').get_text(strip = True)
			result['reply_content'] = reply.find('span', class_ = 'push-content').get_text(strip = True)
			result['reply_replyTime'] = datetime.strptime(reply.find('span', class_ = 'push-ipdatetime').get_text(strip = True), '%m/%d %H:%M')

			results.append(result)
			# list 裡面每個元件都是一個 dictionary
			# print("一篇文的全部留言~~~~", results)

		# 傳一個 list 過去 insert_responses()
		insert_responses(results)

def insert_responses(responses_list):
	# 把每一則的回覆內容抓出來跟資料庫裡面的資料做比對
	for reply in responses_list:
		doc = response_collection.find_one({'reply_content':reply['reply_content']})
		# 如果該筆回覆不在資料庫裡面就新增
		if not doc:
			reply_id = response_collection.insert_one(reply).inserted_id
		# 如果存在則更新該筆資料的內容
		else:
			response_collection.update_one(
				{'_id': reply['reply_id']},
				{'$set':reply},
				upsert = True
			)
			reply_id = reply['reply_id']

	print("文章回覆新增成功!")



if __name__ == "__main__":
	get_post_url(url)


