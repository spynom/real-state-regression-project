import time
import requests
import math

sleep=4
timeout=0
for num in range(315,458):
    while True:
        response=requests.get(f"https://www.makaan.com/noida-residential-property/buy-property-in-noida-city?propertyType=apartment,builder-floor&page={num}")
        if response.status_code==200:
            sleep+=1
            time.sleep(2+(int(math.log(sleep))))
            timeout=0
            print(f"{num} :succesful")
            html=response.text
            with open(f'data/html/{num}.html','w',encoding='utf-8') as f:
                f.write(html)
            break
        else:
            print(f"{num} :server error")
            timeout+=1
            time.sleep(10)
            if timeout==2:
                Print(f"{num}: failed to connect to server and following to next page")
                break
            continue