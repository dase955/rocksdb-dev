import pandas as pd
import lightgbm
import numpy

def train(dataset, output):
    df = pd.read_csv(dataset)
    y = df['Target']
    X = df.drop(columns=['Target'])
    clf = lightgbm.LGBMClassifier()
    clf.fit(X, y)
    # val_pred = clf.predict(X_test)
    clf.booster_.save_model(output)
    
def predict(model, data):
    # df = pd.read_csv("lgb.csv")
    # y = df['Target']
    # X = df.drop(columns=['Target'])
    
    assert type(data) is list 
    data = pd.DataFrame(data).T
    clf = lightgbm.Booster(model_file=model)
    # print(X.loc[0].to_frame().T)
    result = clf.predict(data)
    # print(numpy.argmax(result[0]))
    return numpy.argmax(result[0])
    
if __name__ == '__main__':
    # train()
    predict()