import os
from pathlib import Path
import codecs
import re
import pandas as pd
from datetime import datetime
from fastapi import FastAPI
from sqlalchemy import create_engine
import sqlalchemy


#-----------------------------------------------------------
# print(os.getcwd())
# filepath=""
# if os.path.exists(r"C:/Users/kzarb/Desktop/python_test_solution"):
#     print("yes")
#     filepath = 'C:/Users/kzarb/Desktop/python_test_solution'
# else:
#     print("no") 
    
print(os.getcwd())
filepath=""
if os.path.exists(r"../python_test_solution"):
    print("yes")
    filepath = '../python_test_solution'
else:
    print("no") 
#-----------------------------------------------------------
os.chdir(filepath)
files = [x for x in os.listdir(os.getcwd()) if x.endswith('.txt')]




if __name__ == "__main__":

    files_map = {}
    for current_file in files:
        with codecs.open(current_file, encoding='utf-8') as f:
            lines = [line.strip() for line in f if line.strip()!='']
        text = ' '.join(lines)
        files_map[current_file] = re.sub(" +"," ",text)
        # files_map[current_file] = text

    def extract_websites(text):
        re_website = 'w{3}.[^\s]+'
        re_website_discard = 'www.businessregistry.gr'
        result = [x for x in re.findall(re_website, text) if not x.startswith(re_website_discard)]
        return result[0] if (len(result) == 1) else None

    def extract_gemi(text):
        re_gemi = r'\d{8,12}'
        result = [x for x in list(set(re.findall(re_gemi, text))) if not ((len(x)==10) & (x.startswith('210'))) ]
        return result[0] if (len(result)==1) else None

    def extract_dates(text):
        def most_common(lst):
            return max(set(lst), key=lst.count)

        re_date1 = r'\d{1,2}-\d{1,2}-\d{2,4}'
        re_date2 = r'\d{1,2}\/\d{1,2}\/\d{2,4}'
        re_dates = '|'.join([re_date1, re_date2])
        mc = most_common(re.findall(re_dates, text)).replace('-', '/')  # keep the most frequent
        return datetime.strptime(mc, '%d/%m/%Y')

    def extract_name(text):
        re_name=r'(?<=επωνυμία|ΕΠΩΝΥΜΙΑ)(.*)'
        # re_name=r'επωνυμία\s?«?[.Α-ΩA-Z\s]*'

        result = [x.replace("επωνυμία","").strip() for x in re.findall(re_name,text)]
        return result[0] if (len(result)==1) else None

    df = pd.DataFrame.from_dict(files_map,orient='index').rename(columns={0:'text'})

    df['web'] = df.text.apply(extract_websites)
    df['gemi'] = df.text.apply(extract_gemi)
    df['date'] = df.text.apply(extract_dates)
    df['name'] = df.text.apply(extract_name)
    # df.to_pickle('parsed.pkl')

    print(df['gemi'])



    engine = create_engine('mysql+mysqldb://root:Final_Root_Pass123@localhost/pythontest',pool_pre_ping=True)
    dbConnection = engine.connect()

    df.to_sql('companies',con=engine, schema='pythontest', if_exists='replace', index=False)

    query="SELECT * FROM companies"
    df=pd.read_sql(query,engine)
