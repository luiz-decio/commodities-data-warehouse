import pandas as pd
import random
import dateutil.relativedelta
from datetime import datetime

comodities_list = ["CL=F", "GC=F", "SI=F"]
ref_day = (datetime.now().date())
df_final = None

for n in range(10):

    data_day = ref_day - dateutil.relativedelta.relativedelta(days = n)
    row_n = random.randint(4, 8)

    for _ in range(row_n):
        data = {
            'date' : data_day,
            'symbol' : random.choice(comodities_list),
            'action' : random.choice(['sell', 'buy']),
            'quantity' : random.randint(1, 200)
        }
        df = pd.DataFrame(data=data, index=[0])

        if df_final is None:
            df_final = df 
        else: 
            df_final = pd.concat([df_final, df])

df_final.to_csv("comodities_sell.csv", index=False)