import pandas as pd
import lightgbm
import numpy

model_path = '/pg_wal/ycc/'
# model_path = ''

class LGBModel():
    def __init__(self) -> None:
        self.__model = None
        # one unit is 2 bits-per-key, class = 5 mean bits-per-key = 5 * 2 = 10
        # the default bits-per-key value of previous benchmark is 10
        self.__default_class = 5
        self.__model_name = 'model.txt'
        # self.__host = '127.0.0.1'
        # self.__port = '6666'
        # self.__sock = None
        # self.__server = None
        # normally, one data row will not exceed 1024 Bytes
        # we will check this out in WaLSM c++ client
        # self.__bufsize = 1024
        # self.__conn = 8
        
    def train(self, dataset: str) -> None:
        df = pd.read_csv(dataset)
        y = df['Target']
        X = df.drop(columns=['Target'])
        # clf = lightgbm.LGBMClassifier(min_child_samples=1, n_estimators=1, objective="multiclass")
        clf = lightgbm.LGBMClassifier()
        clf.fit(X, y)
        # if we directly set self.__model = clf, then self.__model always predict class 0
        # we need save clf to txt file, then read this model to init self.__model
        clf.booster_.save_model(model_path + self.__model_name)
        self.__model = lightgbm.Booster(model_file=model_path+self.__model_name)
        print('load a new model')
        
    def predict(self, datas: pd.DataFrame) -> str:
        # currently, only support one data row
        assert len(datas) == 1 
        if self.__model is not None:
            result = self.__model.predict(datas)
            return str(numpy.argmax(result[0]))    
        else:
            return str(self.__default_class)
    
    '''      
    def __close(self) -> None:
        if self.__sock is not None:
            self.__sock.close()
    '''  
    
    '''  
    def start(self) -> None:
        self.__sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        self.__sock.bind((self.__host, self.__port))
        self.__sock.listen(self.__conn)
    '''  
        
    ''' 
    def serve(self) -> None:
        while True:
            client, _ = self.__sock.accept()
            msg = self.__sock.recv(self.__bufsize)
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