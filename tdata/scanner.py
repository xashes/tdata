from tdata import filter as tfilter
from tdata import local
from tqdm import tqdm

from arctic import Arctic

arctic = Arctic('pi3')
basedata = arctic['basedata']
SYMBOLS = basedata.read('instruments').data['symbol'].iloc[5:500]

def scan_first_buy():
    symbols = tqdm(SYMBOLS)

    targets = []
    for symbol in symbols:
        try:
            if tfilter.first_buy(symbol):
                targets.append(symbol)
        except Exception as e:
            pass

    with open('targets.txt', 'w') as fh:
        fh.writelines(targets)


if __name__ == '__main__':
    scan_first_buy()


