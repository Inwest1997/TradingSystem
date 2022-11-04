from lib2to3.pgen2 import token
from src.index import *
from data_loader_old import *
from src.api_practice import *
from src.secret import *

class trading:
    
    def __init__(self): 
        self.call = api_call() 
        self.amount = self.call.amount
        self.token = self.call.ACCESS_TOKEN
        self.date = dt.datetime.now().strftime('%Y-%m-%d').replace('-','')
        self.거래df = pd.DataFrame()
        # self.df = self.get_data(self)


    def get_date_price(self, ticker):
        # get_current_price
        ''' Return date and price for bar.
        '''
        self.price = self.call.get_current_price(ticker)

        return self.date, self.price
    

    # buy_signal/ sell_signal
    #     - 백테스팅에서 시그널 받아 오기 
    #     - place_buy_order / place_sell_order 실행 
    def re_signal(self):
        # sig 받아오는 함수 


        if self.sig == 1 :
            self.call.buy_order(self, self.ticker )

        elif self.sig == -1:
            self.call.sell_order(self, self.ticker )

        else:
            print('잘못된 주소입니다.')


    
    # place_buy_order / place_sell_order 
    #     - 매수, 매도하는 날의 날짜 및 가격 불러오기 
    #     - 한투 api 에서 매수, 매도 
    #     - 한투 api 에서 잔액, 거래량(매수, 매도 양) 불러오기 
    #     - 거래횟수 카운트 
    def buy_order(self, ticker ):
        # 한투api 연동, 매수 
        self.call.get_order_buy(ticker, 1 )
        self.call.get_inquire_ccn(self.date)
        df = '생성'
        정보_저장(df)
    

    def sell_order(self, ticker ):
        

        self.call.get_order_buy(ticker, -1 )
        self.call.get_inquire_ccn(self.date)
        정보_저장(df)
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    def 정보_저장(self,df):
        if len(self.거래df) == 0:
            self.거래df = df
        else:
            pd.concat([self.거래df,df], axis=1)
    # 거래데이터
    # 날짜(한투), 몇주(한투), 종목(한투), 종목별 수익률(계산), 매입가(한투), 종가(한투)
    def 거래데이터(self):
        self.date                                       #날짜
        self.call.get_present_balance()            #종목, 매수량, 매도량, 모든거래 종료후 잔액 , 해당종목의 현재가(종가), 헤당 종목의 매수 평균단가
        self.전체df     
    # close_out 
    # 날짜(한투) 예수금(한투), 잔액(한투), 현재가() , 전체 수익률(계산)
    def close_out(self):
        # db업로드 전체df, 거래df
        pass