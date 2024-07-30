import pandas as pd
import lightgbm
import numpy

class LGBModel():
    def __init__(self) -> None:
        self.__model = None
        # self.__host = '127.0.0.1'
        # self.__port = '6666'
        # self.__socket = None
        # self.__server = None
        # normally, one data row will not exceed 1024 Bytes
        # we will check this out in WaLSM c++ client
        # self.__bufsize = 1024
        # self.__conn = 8
        
    def train(self, dataset: str) -> None:
        df = pd.read_csv(dataset)
        y = df['Target']
        X = df.drop(columns=['Target'])
        clf = lightgbm.LGBMClassifier()
        clf.fit(X, y)
        self.__model = clf
        
    def predict(self, datas: pd.DataFrame) -> str:
        # currently, only support one data row
        assert len(datas) == 1 
        assert self.__model is not None
        result = self.__model.predict(datas)
        return str(numpy.argmax(result[0]))    
    
    '''      
    def __close(self) -> None:
        if self.__socket is not None:
            self.__socket.close()
    '''  
    
    '''  
    def start(self) -> None:
        self.__socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        self.__socket.bind((self.__host, self.__port))
        self.__socket.listen(self.__conn)
    '''  
        
    ''' 
    def serve(self) -> None:
        while True:
            client, _ = self.__socket.accept()
            msg = self.__socket.recv(self.__bufsize)
            decoded = lgb_util.parse_msg(msg)
            
            if type(decoded) is str:
                # send client nothing
                self.__train(decoded)
            elif type(decoded) is list:
                decoded = lgb_util.prepare_data(decoded)
                # self.__predict(decoded)
                # send client target class str (like '0' or '1' or ... )
                client.send(self.__predict(decoded))
            else:
                print('msg type unknown, LGBServer exit')
                self.__close()
    '''    