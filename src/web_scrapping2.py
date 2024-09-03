import time
import requests
import math
from bs4 import BeautifulSoup
import pandas as pd


links=[]
print("----------------------------------------------------")
print("extracting links from html pages")
print("----------------------------------------------------")
for page in range(1,458):
    with open(f"data/html/{page}.html","r") as file:
        html=BeautifulSoup(file,"lxml")
    for element in html.find_all('li',class_="cardholder"):
        links.append(element.find("div",class_="cardLayout clearfix").get("itemid"))
        print("extracted till now: ",len(links))
print("----------------------------------------------------")
print("total extracted: ", len(links))
print("----------------------------------------------------")        
print("scrapping data started")
print("----------------------------------------------------") 

data1={
    "link":[],"name":[],"size":[],"address":[],"price":[],"rate":[]
}
data2={}
data3={}
data4={}
num=0
timeout=0

for num in range(1,len(links)+1):
    
    #if not num%100:
    #    time.sleep(10)
    
    while True:
        link=links[num-1]
        response=requests.get(link)    
        if response.status_code==200:
            data1["link"].append(link)
            try:
                page=BeautifulSoup(response.text,"lxml")
            except:
                page=""
            #sleep+=1
            time.sleep(1)
            timeout=0
            try:
                head=page.find("h1",class_="type-wrap")
            except:
                head=""

            try:
                name=head.find("span",class_="type").text.strip()
            except:
                name=""
            data1["name"].append(name)

            try:
                size=head.find("span",class_="size").text.strip()
            except:
                size=""
            data1["size"].append(size)

            try:
                address=head.find("div",class_="loc-wrap").text.strip()
            except:
                address=""
            data1["address"].append(address)

            try:
                head2=page.find("div",class_="i-row clearfix")
            except:
                head2=""
            try:
                price_details=head2.find("div",class_="i-lcol")
            except:
                price_detail=""
            try:
                detail=head2.find("div",class_="i-rcol").find_all("tr",class_="ditem")
            except:
                detail=" "
            try:
                price=price_details.find("div",class_="price-wrap").text
            except:
                price=""
            data1["price"].append(price)

            try:
                rate=price_details.find("div",class_="rate-wrap").text
            except:
                rate=""
            data1["rate"].append(rate)


            try:
                keys = [ element.find("td",class_="lbl").text for element in detail ]
                values= [ element.find("td",class_="val").text for element in detail ]
                for col in keys:
                    if col not in list(data2.keys()):
                        if num ==1:
                            data2[col]=[]
                        else:
                            data2[col]=(num-1)*[""]
                for x in range(len(values)):
                    data2[keys[x]].append(values[x])
                for key in data2:
                    if key not in keys:
                        data2[key].append("")
            except:
                for key in data2:
                    data2[key].append("")

            try:
                other_details=page.find("div",class_="info-card").find("table",class_="kd-list js-list").find_all("tr",class_="listitem")
            except:
                other_details=""
            try:
                keys = [ element.find("td",class_="lbl").text for element in other_details ]
                values= [ element.find("td",class_="val").text for element in other_details ]
                for col in keys:
                    if col not in list(data3.keys()):
                        if num ==1:
                            data3[col]=[]
                        else:
                            data3[col]=(num-1)*[""]
                for x in range(len(values)):
                    data3[keys[x]].append(values[x])
                for key in data3:
                    if key not in keys:
                        data3[key].append("")
            except:
                for key in data3:
                    data3[key].append("")

            try:
                values=[]
                for elements in page.find_all("div",class_="icons-list js-list js-mobscroll"):
                    values.append(str([element.text.strip().replace("\\","").replace("'","").replace("'","") for element in elements.find_all("div",class_="js-moblist-item") if "Not Available" not in element.text]).replace("'",""))
                keys=[str(x) for x in range(1,len(values)+1)]
                for col in keys:
                    if col not in list(data4.keys()):
                        if num ==1:
                            data4[col]=[]
                        else:
                            data4[col]=(num-1)*[""]
                for x in range(len(values)):
                    data4[keys[x]].append(values[x])
                for key in data4:
                    if key not in keys:
                        data4[key].append("")
            except:
                for key in data4:
                    data4[key].append("")
            name_=data1["name"][-1]
            print(f"{num} :{name_}")
            break
        else:
            if timeout==1:
                print(f"{num}: failed to connect to server and following to next page")
                break
            print(f"{num} :server error")
            timeout+=1
            time.sleep(20)
            continue
    if not num%1000:
        df=pd.concat([pd.DataFrame(data1),pd.DataFrame(data2),pd.DataFrame(data3),pd.DataFrame(data4)],axis=1)
        df.to_csv(f"data/raw/scrapped_data_sample{num/1000}.csv",index=False)
print("------------------------")
print("scrapping data done")
print("------------------------")
df=pd.concat([pd.DataFrame(data1),pd.DataFrame(data2),pd.DataFrame(data3),pd.DataFrame(data4)],axis=1)
print(f"data_shape: {df.shape}")
print("------------------------")
df.to_csv("data/raw/scrapped_data.csv",index=False)

            
