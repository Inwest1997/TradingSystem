import requests
import json
from src.secret import *
#있어야할 메뉴들 생각중
# 기초정보 확인 (액세스토큰 발급 )   ( 완료 )
# 1. 잔고조회   (일단 완료)
# 2. 주식현재체결가 ( 일단 완료)
# 3. 해외주식 기간별시세
#
# 2022-10-27 업데이트
# 예약주문접수취소, 정정취소주문 예약주문접수 해외주식 주문, 주문체결내역, 체결기준현재잔고
# 미체결내역, 주야간원장구분조회,
#
# 2022-10-28 업데이트
# 개인정보 secret.py로 이동
# 모듈화 과정 작업중
# 현물계좌 연결테스트시 작동완료
# 매수매도는 아직 미테스트
#
###############################################################################


#---------------------------------------------
# 기본 정보관련
#---------------------------------------------




# -----------------------------------------------------------
# HASHKEY 발급
# 발급하기위해서 필요한 데이터들
#
# "CANO": "30081099",
# "ACNT_PRDT_CD": "01",
# "OVRS_EXCG_CD": "NASD",
# "PDNO": "AAPL",
# "ORD_QTY": "1",
# "OVRS_ORD_UNPR": "145.00",
# "CTAC_TLNO": "",
# "MGCO_APTM_ODNO": "",
# "ORD_SVR_DVSN_CD": "0",
# "ORD_DVSN": "00"
#

#------------------------------------------------------------
#--------------------------------------------테스트하는곳
datas = {
"CANO": CANO,
"ACNT_PRDT_CD": ACNT_PRDT_CD,
"OVRS_EXCG_CD": "NASD",
"PDNO": "AAPL",
"ORD_QTY": "1",
"OVRS_ORD_UNPR": "145.00",
"CTAC_TLNO": "",
"MGCO_APTM_ODNO": "",
"ORD_SVR_DVSN_CD": "0",
"ORD_DVSN": "00"
}

class api_call:

    def __init__(self,ticker='GOOG'):
        self.ticker = ticker
        self.initial_amount = AMOUNT
        self.amount = self.get_cash_balance()
        self.trades = 0
        self.datas = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "OVRS_EXCG_CD": "NASD",
            "PDNO": "AAPL",
            "ORD_QTY": "1",
            "OVRS_ORD_UNPR": "145.00",
            "CTAC_TLNO": "",
            "MGCO_APTM_ODNO": "",
            "ORD_SVR_DVSN_CD": "0",
            "ORD_DVSN": "00"
            }
        self.ACCESS_TOKEN = self.get_access_token()
        self.HASHKEY = self.hashkey()
        
    


    def hashkey(self):
        PATH = "uapi/hashkey"
        URL = f"{URL_BASE}/{PATH}"
        headers = {
        'content-Type' : 'application/json',
        'appKey' : APP_KEY,
        'appSecret' : APP_SECRET,
        }
        res = requests.post(URL, headers=headers, data=json.dumps(self.datas))
        HASHKEY = res.json()["HASH"]

        return HASHKEY

    # -----------------------------------------------------------
    # ACCESS TOKEN 발급
    #------------------------------------------------------------
    def get_access_token(self):                                 # POST 방식
        headers = {"content-type":"application/json"}       # 기본정보(Content-Type)
        body = {"grant_type":"client_credentials",          # R : 권한부여 타입
        "appkey":APP_KEY,
        "appsecret":APP_SECRET}
        PATH = "oauth2/tokenP"                              # 기본정보(URL) : URL
        URL = f"{URL_BASE}/{PATH}"                          # 기본정보(도메인) : https://openapivts.koreainvestment.com:29443/oauth2/tokenP
        res = requests.post(URL, headers=headers, data=json.dumps(body))	# Response 데이터 호출
        ACCESS_TOKEN = res.json()["access_token"]
        return ACCESS_TOKEN


    #-------------------------------------------------------
    # 해외주식 잔고 조회 (모의에선 안되는거같음)
    # 실전 Domain : "https://openapivts.koreainvestment.com:9443"
    # 모의 Domain : "https://openapivts.koreainvestment.com:29443"
    # URL : /uapi/overseas-stock/v1/trading/inquire-balance
    #-------------------------------------------------------
    def get_cash_balance(self):
        PATH = "/uapi/overseas-stock/v1/trading/inquire-balance"
        URL = f"{URL_BASE}/{PATH}"
        headers = {"Content-Type":"application/json",
            "authorization":f"Bearer {self.get_access_token()}",
            "appKey":APP_KEY,
            "appSecret":APP_SECRET,
            "tr_id":"JTTT3012R",        # [실전투자] JTTT3012R  [모의투자] VTTT3012R : 야간용 // VTTS3012R : 주간용
            #"custtype":"P",             # 고객타입 : "P" : 개인   // "B" : 법인
        }
        params = {
            "CANO": CANO,                   # 계좌 앞 8자리
            "ACNT_PRDT_CD": ACNT_PRDT_CD,   # 계좌 뒤 2자리
            "OVRS_EXCG_CD": "NAS",          # 해외거래소코드 : NASD (미국전체) , NAS (나스닥)
            "TR_CRCY_CD": "USD",            # 거래통화코드 (USD 달러)
            "CTX_AREA_FK200": "",           # 최초 조회시는 공란
            "CTX_AREA_NK200": "",           # 최초 조회시는 공란
        }
        res = requests.get(URL, headers=headers, params=params)
        print(res.json())
        # balance = (int)(res.json())   # OVRS_CBLC_QTY : 해외잔고수량 ['ovrs_cblc_qty']
        # send_message(f"주문 가능 현금 잔고: {balance}원")
        # print(f"주문 가능 현금 잔고: {balance}원")
        AMOUNT = res.json()['output1']
        # print("잔고:", self.amount)
        # return int(balance)
        return AMOUNT


    #---------------------------------------------------------------
    # 해외주식 현재가 시세
    # 아마 모의투자는 지원안하는듯
    #---------------------------------------------------------------
    def get_current_price(self):			# 애플 코드 : AAPL / 테슬라 코드 : TSLA
        PATH = "/uapi/overseas-price/v1/quotations/price"
        URL = f"{URL_BASE}/{PATH}"
        headers = {"Content-Type":"application/json",
                "authorization": f"Bearer {ACCESS_TOKEN}",
    #           OAuth 토큰이 필요한 API 경우 발급한 Access token
    #           일반고객(Access token 유효기간 1일, OAuth 2.0의 Client Credentials Grant 절차를 준용)
    #           법인(Access token 유효기간 3개월, Refresh token 유효기간 1년, OAuth 2.0의 Authorization Code Grant 절차를 준용)
                "appKey": APP_KEY,
                "appSecret": APP_SECRET,
                "tr_id":"HHDFS00000300"}            # 거래 ID - HHDFS00000300 : 주식 현재가
        # Query Parameter
        params = {
        "AUTH":"",               # 사용자권한정보 "" (NULL 값 설정)
        "EXCD":"NAS",            # 거래소코드 : NAS (나스닥)
        "SYMB" : self.ticker            # 입력 종목코드 :  code ( 현재 AAPL : 애플로 설정되어있음)
        }
        res = requests.get(URL, headers=headers, params=params)

        return res.json()['output']['last']


    #---------------------------------------------------------------
    # Method : GET
    # 해외주식 기간별 시세
    # 해외주식의 기간별시세를 확인하는 API
    # 아마 모의투자는 지원안하는듯
    #---------------------------------------------------------------
    def get_day_price(self):			# 애플 코드 : AAPL / 테슬라 코드 : TSLA
        PATH = "/uapi/overseas-price/v1/quotations/dailyprice"
        URL = f"{URL_BASE}/{PATH}"
        headers = {"Content-Type":"application/json",
                "authorization": f"Bearer {ACCESS_TOKEN}",
    #           OAuth 토큰이 필요한 API 경우 발급한 Access token
    #           일반고객(Access token 유효기간 1일, OAuth 2.0의 Client Credentials Grant 절차를 준용)
    #           법인(Access token 유효기간 3개월, Refresh token 유효기간 1년, OAuth 2.0의 Authorization Code Grant 절차를 준용)
                "appKey": APP_KEY,
                "appSecret": APP_SECRET,
                "tr_id":"HHDFS00000300",        # 거래 ID - HHDFS00000300 : 주식 현재가 / 모의투자는 미제공 현재가라 상관없어서 넣어둠
                "custtype" : "P",
                    #맥어드레스
                    #연락처 필요
                "hashkey": self.HASHKEY
                    }
        # Query Parameter
        params = {
        "AUTH":"",              # 사용자권한정보 "" (NULL 값 설정)
        "EXCD":"NAS",           # 거래소코드 : NAS (나스닥)
        "SYMB" : self.ticker,          # 입력 종목코드 :  code ( 현재 AAPL : 애플로 설정되어있음)
        "GUBN" : "1",           # 일주월 구분 == 일 : 0 / 주 : 1 / 월 : 2
        "BYMD" : "",            # 조회기준일자 공백시 오늘 날짜로 지정
        "MODP" : "0"            # 수정주가반영여부 == 미반영 : 0 / 반영 : 1

        }
        res = requests.get(URL, headers=headers, params=params)

        return res.json()['output']['last']




    #---------------------------------------------------------------
    # Method : POST
    # 해외주식 주문-001
    # 해외거래소 운영시간(한국시간 기준)
    # 미국 23:30 ~ 06:00 (썸머타임 22:30 ~ 05:00)
    #---------------------------------------------------------------
    def get_order_buy(self,  signal =0 ):			# 애플 코드 : AAPL / 테슬라 코드 : TSLA
        PATH = "/uapi/overseas-stock/v1/trading/order"
        URL = f"{URL_BASE}/{PATH}"
        headers = {"Content-Type":"application/json",
                    "authorization": f"Bearer {ACCESS_TOKEN}",
                    "appKey": APP_KEY,
                    "appSecret": APP_SECRET,
                    "tr_id":"VTTT1002U",
                    # 실전투자
                    # 매수 : JTTT1002U / 매도 : JTTT1006U
                    # 모의투자
                    # 매수 : VTTT1002U / 매도 : VTTT1001U
                    }
        if signal == -1 :
            headers["tr_id"]  = 'JTTT1006U'
        elif signal == 1:
            headers["tr_id"]  = 'JTTT1002U'
        else:
            headers["tr_id"] = 'error'
            print('잘못입력하셨습니다.')
        # Query Parameter
        params = {
        "CANO": CANO,           # 계좌번호
        "ACNT_PRDT_CD" : ACNT_PRDT_CD,
        "OVRS_EXCG_CD":"NASD",           # 거래소코드 : NASD (나스닥)
        "PDNO" : self.ticker,          # 상품번호( 종목코드)
        "ORD_QTY" : "1",        #  주문수량
        "OVRS_ORD_UNPR" : "0",            # 1주당 주문단가 / 시장가의 경우 : "0" 입력

        }
        res = requests.get(URL, headers=headers, params=params)

        #일단 현재 호출되는지로만 체크해둠 함수만들어두고,
        return res.json()['msg1']



    #---------------------------------------------------------------
    # Method : POST
    # 해외주식 예약주문접수
    # 해외거래소 운영시간(한국시간 기준)
    # 미국 23:30 ~ 06:00 (썸머타임 22:30 ~ 05:00)
    #---------------------------------------------------------------
    def get_reserve_order(self):			# 애플 코드 : AAPL / 테슬라 코드 : TSLA
        PATH = "/uapi/overseas-stock/v1/trading/order-resv"
        URL = f"{URL_BASE}/{PATH}"
        headers = {"Content-Type":"application/json",
                    "authorization": f"Bearer {ACCESS_TOKEN}",
                    "appKey": APP_KEY,
                    "appSecret": APP_SECRET,
                    "tr_id":"VTTT3014U",
                    # 실전투자
                    # 매수 : JTTT3014U / 매도 : JTTT3016U
                    # 모의투자
                    # 매수 : VTTT3014U / 매도 : VTTT3016U
                    }
        # Query Parameter
        params = {
        "CANO": CANO,           # 계좌번호
        "ACNT_PRDT_CD" : ACNT_PRDT_CD,
        "PDNO" : self.ticker,            #상품번호 종목코드
        "OVRS_EXCG_CD":"NASD",           # 거래소코드 : NASD (나스닥)
        "FT_ORD_QTY" : "1",        #  주문수량
        "FT_ORD_UNPR3" : "110",            # 1주당 예약주문단가

        }
        res = requests.get(URL, headers=headers, params=params)

        #일단 현재 호출되는지로만 체크해둠 함수만들어두고,
        return res.json()['msg1']




    #---------------------------------------------------------------
    # Method : POST
    # 해외주식 정정취소주문
    # 해외거래소 운영시간(한국시간 기준)
    # 미국 23:30 ~ 06:00 (썸머타임 22:30 ~ 05:00)
    #---------------------------------------------------------------
    def get_reserve_edit(self):			# 애플 코드 : AAPL / 테슬라 코드 : TSLA
        PATH = "/uapi/overseas-stock/v1/trading/order-rvsecncl"
        URL = f"{URL_BASE}/{PATH}"
        headers = {"Content-Type":"application/json",
                    "authorization": f"Bearer {ACCESS_TOKEN}",
                    "appKey": APP_KEY,
                    "appSecret": APP_SECRET,
                    "tr_id":"JTTT1004U",
                    # 실전투자
                    # 미국 정정 취소 주문 : JTTT1004U
                    # 모의투자
                    # 미국 정정 취소 주문 : VTTT1004U /
                    }
        # Query Parameter
        params = {
        "CANO": CANO,           # 계좌번호
        "ACNT_PRDT_CD" : ACNT_PRDT_CD,
        "OVRS_EXCG_CD":"NASD",           # 거래소코드 : NASD (나스닥)
        "PDNO": self.ticker,               # 상품번호
        "ORGN_ODNO" : "",           #  원주문번호 : 정정 또는 취소할 원주문번호(해외주식_주문 API ouput ODNO 참고)
        "RVSE_CNCL_DVSN_CD" : "01",            # 01 : 정정 / 02 : 취소
        "ORD_QTY" : "3",                # 주문수량
        "OVRS_ORD_UNPR" : " 102"        # 주문 단가
        }
        res = requests.get(URL, headers=headers, params=params)

        #일단 현재 호출되는지로만 체크해둠 함수만들어두고,
        return res.json()['msg1']


    #---------------------------------------------------------------
    # Method : POST
    # 해외주식 예약주문접수 취소
    # 해외거래소 운영시간(한국시간 기준)
    # 미국 23:30 ~ 06:00 (썸머타임 22:30 ~ 05:00)
    # 모의투자 미지원
    #---------------------------------------------------------------
    def get_reserve_cancel(self, order_code="2103021032"):	# 길이10 - 주문번호
        PATH = "/uapi/overseas-stock/v1/trading/order-resv-ccnl"
        URL = f"{URL_BASE}/{PATH}"
        headers = {"Content-Type":"application/json",
                    "authorization": f"Bearer {ACCESS_TOKEN}",
                    "appKey": APP_KEY,
                    "appSecret": APP_SECRET,
                    "tr_id":"JTTT3017U",
                    # 실전투자
                    # 미국 취소 주문 : JTTT3017U
                    }
        # Query Parameter
        params = {
        "CANO": CANO,           # 계좌번호
        "ACNT_PRDT_CD" : ACNT_PRDT_CD,
        "RSYN_ORD_RCTT_DT":"20221010",           # 해외주문 접수일자
        "OVRS_RSVN_ODNO": order_code,               # 해외주식 예약주문접수 API OUTPUT 참고
        }
        res = requests.get(URL, headers=headers, params=params)

        #일단 현재 호출되는지로만 체크해둠 함수만들어두고,
        return res.json()['msg1']


    #---------------------------------------------------------------
    # Method : GET
    # 해외주식 주문체결내역
    # 해외거래소 운영시간(한국시간 기준)
    # 미국 23:30 ~ 06:00 (썸머타임 22:30 ~ 05:00)
    # 모의투자 미지원
    #---------------------------------------------------------------
    def get_inquire_ccn(self, order_code="2103021032"):
        PATH = "/uapi/overseas-stock/v1/trading/inquire-ccn"
        URL = f"{URL_BASE}/{PATH}"
        headers = {"Content-Type":"application/json",
                    "authorization": f"Bearer {ACCESS_TOKEN}",
                    "appKey": APP_KEY,
                    "appSecret": APP_SECRET,
                    "tr_id":"JTTT3017U",
                    # 주야간 원장 구분 해야함
                    # 실전투자
                    # JTTT3001R : PSBL_YN(주야간 원장 구분) = 'Y' (야간용)
                    # TTTS3035R : PSBL_YN(주야간 원장 구분) = 'N' (주간용)
                    # 모의투자
                    # VTTT3001R : 야간용
                    # VTTS3035R : 주간용
                    }
        # Query Parameter
        params = {
        "CANO": CANO,           # 계좌번호
        "ACNT_PRDT_CD" : ACNT_PRDT_CD,
        "PDNO" : "",            # 상품번호 ( 공백 전종목)  아래로 세개 함수 파라미터해야할듯
        "ORD_STRT_DT":"20221010",           # 주문시작일자
        "ORD_END_DT": "20221014",       # 주문종료일자
        "SLL_BUY_DVSN" : "00",          # 00 : 전체  / 01 : 매도  / 02 : 매수
        "CCLD_NCCS_DVSN" : "00",          # 00 : 전체  / 01 : 매도  / 02 : 매수
        "OVRS_EXCG_CD" : "NASD",
        "SORT_SQN" : "DS",              # 정렬순서   DS : 정순 / AS : 역순
        "ORD_DT" : "",                  #NULL 값설정
        "ORD_GNO_BRNO" : "",                  #NULL 값설정
        "ODNO" : "",                  #NULL 값설정
        "CTX_AREA_NK200" : "",
        "CTX_AREA_FK200" : ""
        }
        res = requests.get(URL, headers=headers, params=params)

        #일단 현재 호출되는지로만 체크해둠 함수만들어두고,
        return res.json()['msg1']


    #---------------------------------------------------------------
    # Method : GET
    # 해외주식 체결기준현재잔고
    # 해외거래소 운영시간(한국시간 기준)
    # 미국 23:30 ~ 06:00 (썸머타임 22:30 ~ 05:00)
    # 모의투자 미지원
    #---------------------------------------------------------------
    def get_present_balance(self, order_code="2103021032"):
        PATH = "/uapi/overseas-stock/v1/trading/inquire-present-balance"
        URL = f"{URL_BASE}/{PATH}"
        headers = {"Content-Type":"application/json",
                    "authorization": f"Bearer {ACCESS_TOKEN}",
                    "appKey": APP_KEY,
                    "appSecret": APP_SECRET,
                    "tr_id":"JTTT3017U",
                    # 주야간 원장 구분 해야함
                    # 실전투자 : CTRP6504R
                    # 모의투자 : VTRP6504R
                    }
        # Query Parameter
        params = {
        "CANO": CANO,           # 계좌번호
        "ACNT_PRDT_CD" : ACNT_PRDT_CD,
        "WCRC_FRCR_DVSN_CD" : "02",            # 01 : 원화   02 : 외화
        "NATN_CD":"840",           # 국가코드 : 000 전체  840 미국
        "TR_MKET_CD": "01",       # 거래시장코드  미국 선택시 : 00 : 전체 / 01 : 나스닥 02 뉴욕거래서
        "INQR_DVSN_CD" : "00",          # 조회구분코드  00 : 전체  / 01 : 일반해외주식  / 02 : 미니스탁
        }
        res = requests.get(URL, headers=headers, params=params)

        #일단 현재 호출되는지로만 체크해둠 함수만들어두고,
        return res.json()['msg1']




    # print("엑세스토큰 : " + ACCESS_TOKEN)
    # print(get_current_price())