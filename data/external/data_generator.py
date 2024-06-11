import pandas as pd
import random
import dateutil.relativedelta
from datetime import datetime

comodities_list = ["CL=F", "GC=F", "SI=F"]
ref_day = (datetime.now().date())
df_generated_data = None

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

        if df_generated_data is None:
            df_generated_data = df 
        else: 
            df_generated_data = pd.concat([df_generated_data, df])

df_generated_data.to_csv("comodities_sell.csv", index=False)