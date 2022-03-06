import datetime

import meteoCocito

# Example

# Remember to `pip install -r requirements.txt`

meteoCocito.DataArchive.update()

for excerpt in meteoCocito.TextGenerator.day():
    print(excerpt)

data = meteoCocito.DataArchive.current()

print(data)

elenco = meteoCocito.DataArchive.betweenDatetimes(datetime.datetime(2022, 1, 1), datetime.datetime(2022, 2, 1))

for valore in elenco:
    if valore.symbol == meteoCocito.DataTypeArchive.Symbols.temperature:
        print(valore.instant, float(valore))
