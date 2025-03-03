# 最初に必要なライブラリをimportする！これ鉄則！
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service

# 引数の設定（一部、各自で要変更）
driver_path='chromedriverの保存先' #要変更
url_lifevision="https://ww1.tcclog.jp/lv_rise/index.asp"
corp='RU'
user='LifeVisionのID' #要変更
pw='LifeVisionのパスワード' #要変更

# Chrome用ドライバーの呼び出し
service = Service(executable_path=driver_path)
driver=webdriver.Chrome(service=service)

# URLへアクセス
driver.get(url_lifevision)

# ログインのため、必要事項
ru_cd = driver.find_element(By.ID, "txtCorp")
my_cd = driver.find_element(By.ID, "txtUser")
password = driver.find_element(By.ID, "txtPass")
submit = driver.find_element(By.NAME, "imgLogonBtn")

#入力欄を空にする
my_cd.clear()
password.clear()

#IDとパスワードを入力
ru_cd.send_keys(corp)
my_cd.send_keys(user)
password.send_keys(pw)

# ログインボタン押下
submit.click()

# ちょっと待機
time.sleep(5)

# ドライバーを閉じる（プログラム終了）
driver.close()
