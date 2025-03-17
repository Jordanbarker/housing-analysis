import pandas as pd

def load_train_test():
    train_df = pd.read_csv(r'..//data//train.csv')
    test_df = pd.read_csv(r'..//data//test.csv')
    
    train_df = prepare_data(train_df)
    test_df = prepare_data(test_df)
    
    return train_df, test_df