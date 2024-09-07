import os
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

def get_data(*args: str) -> pd.DataFrame:
    # Join the arguments into a file path
    path:str = os.path.join(*args)
    # Read and return the CSV file as a DataFrame
    return pd.read_csv(path)

# loading data
flat_data=get_data("data","raw","scrapped_data.csv")

#defining function to clean data
def data_clean(df:pd.DataFrame)->pd.DataFrame:

    df_= (
        df
        .assign(   
            bedroom=df["name"].str.extract(r"(\d+)"),
            type1=df["name"].str.split(" ",n=1).str.get(1).str.extract(r"([A-Za-z]+)"),
            type2=df["name"].str.split(" ",n=1).str.get(1).str.replace(r"^([A-Za-z]+\s)","",regex=True),
            size=df["size"].str.replace(",","").str.extract(r"(\d+)"),
            address=(df.address.str.lower().str.replace(r'[A-Za-z0-9\s]*?4 greater noida wes+[A-Za-z0-9\s]*',"4 greater noida west",regex=True)
                    .str.replace(r'[A-Za-z0-9\s]*?(extension|ext)[A-Za-z0-9\s]*',"extension, noida",regex=True)
                    .str.replace(r'[A-Za-z0-9\s]*?ambedkar city[A-Za-z0-9\s]*',"ambedkar city, noida",regex=True)
                    .str.replace(r'[A-Za-z0-9\s]*?yamuna expressway[A-Za-z0-9\s]*',"yamuna expressway, noida",regex=True)
                    .str.replace("surajpur site 4, noida","surajpur site 4, greater noida")
                    .str.replace("knowledge park 3, noida","knowledge park 3, greater noida")
                    .str.replace("sikandarpur village, noida","sector 87, noida")
                    .str.replace("dallupura, noida","sector 10, noida")
                    .str.replace("pristine avenuegaur city road, noida","gaur city, greater noida")
                    .str.replace("migsun vilaasaeta ii, noida","s block, greater noida")
                    .str.replace("nirala greenshiregreater noida west road, noida","greater noida west road, noida")
                    .str.replace("home and soul f premiereyeida, noida","jal vayu vihar, noida")
                    .str.replace("apte gra indraprastha phase 2shramik kunj, noida","sector 93, noida")
                    .str.replace("apte gra indraprastha phase 2shramik kunj, noida","sector 93, noida")
                    .str.replace(r'[0-9a-z ]+sector',"sector" ,regex=True).str.replace("-",'')),
            price_in_lakh=((df["price"].str.replace("EMI","").str.extract(r"(Cr|L)").replace({"L":"1","Cr":"100"}).astype(float))*
                            (df["price"].str.replace("EMI","").str.extract(r"([0-9.]+)").astype(float))),
            rate= pd.to_numeric(  df["rate"].str.extract(r"([0-9,]+)").loc[:,0].str.replace(",","")),
            carpet_area= pd.to_numeric(  df["Carpet area"].str.extract(r"([0-9,]+)").loc[:,0].str.replace(",","")),
            age_of_property=(df["Age of Property"].fillna("")+df["Age of Property.1"].fillna("")).str.extract(r"([0-9]+ y)").loc[:,0].str.replace(" y",""),
            floor=pd.to_numeric(df["Floor"].str.replace("Gr","0").str.extract(r"(\d+)").iloc[:,0]),
            total_floor=pd.to_numeric(df["Floor"].str.extract(r"(\s\d+)").iloc[:,0]),
            additional_rooms=pd.to_numeric(((df["Additional Rooms"].fillna(""))+(df["Additional Rooms.1"].fillna(""))).str.replace(r"\([a-z\s,]+\)","",regex=True).str.extract(r"(\d+)").loc[:,0]),
            price_negotiable=(df["Price Negotiable"].fillna("")+df["Price Negotiable.1"].fillna("")),
            balconies=(df["Balconies"].fillna(0)+df["Balconies.1"].fillna(0)).replace(0,np.nan),
            type_of_sale=(df["New/Resale"].fillna(""))+(df["New/Resale.1"].fillna("")),
            booking_amount_in_lakh =(pd.to_numeric(df["Booking Amount"].str.replace(",",""))/100000)
            )
                      
    )


    temp1=df["1"].replace(r"[]",np.nan).str.lower().str.replace(", ",",").str.replace(r"lift(s)","lift").str.replace(r"\[|\]","",regex=True).str.split(",",expand=True)
    l=[]
    for col in range(24):
        for row in range(9132):
            x=temp1.iloc[row,col]
            if x not in l:
                l.append(x)
    l.remove(np.nan)
    l.remove(None)
    temp1=pd.DataFrame({f"amenities_{e}":df["1"].replace(r"[]",np.nan).str.lower().str.contains(f"{e}") for e in l})

    temp2=df["2"].replace(r"[]",np.nan).str.lower().str.replace(", ",",").str.replace(r"lift(s)","lift").str.replace(r"\[|\]","",regex=True).str.split(",",expand=True)
    l=[]
    for col in range(11):
        for row in range(9132):
            x=temp2.iloc[row,col]
            if x not in l:
                l.append(x)
    l.remove(np.nan)
    l.remove(None)
    temp2=pd.DataFrame({f"furnish_detail_{e}":df["2"].replace(r"[]",np.nan).str.lower().str.contains(f"{e}") for e in l})


    df_=(
        pd.concat([df_,temp1,temp2],axis=1).rename(columns={"Status.1":"Furnishing Status","Status":"Property Status"})
        .rename(columns=str.lower)
        .drop(columns=["price","name","carpet area","age of property","age of property.1","additional rooms","additional rooms.1","price negotiable","price negotiable.1","balconies.1","new/resale",
                            "new/resale.1","link","name","1","2","floor"])
    )
    df_.columns = [col.replace(' ', '_') for col in df_.columns]
    return df_

clean_data=data_clean(flat_data)


def save_data(dir_: list, dataset: pd.DataFrame, train_size: float = 0.8, test_size: float = 0.1) -> None:
    # Ensure the directory exists
    path = os.path.join(*dir_)
    if not os.path.exists(path):
        os.makedirs(path)

    # Separate features and target variable
    X = dataset.drop(columns=["price_in_lakh"])
    y = dataset["price_in_lakh"]

    # Check if train_size and test_size are valid
    if train_size + test_size >= 1:
        raise ValueError("train_size and test_size should be such that train_size + test_size < 1")

    # Split data into training and test sets
    X_train, X_temp, y_train, y_temp = train_test_split(X, y, train_size=train_size)

    # Calculate validation size
    val_size = 1 - train_size - test_size
    X_test, X_val, y_test, y_val = train_test_split(X_temp, y_temp, train_size=(test_size / (test_size + val_size)))

    # Save data to CSV files
    pd.concat([X_train, y_train], axis=1).to_csv(os.path.join(path, "train.csv"), index=False)
    pd.concat([X_test, y_test], axis=1).to_csv(os.path.join(path, "test.csv"), index=False)
    pd.concat([X_val, y_val], axis=1).to_csv(os.path.join(path, "val.csv"), index=False)


save_data(["data","clean"],dataset=clean_data)



        
