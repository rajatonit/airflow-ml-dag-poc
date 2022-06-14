import numpy as np
import pandas as pd
from sklearn.datasets import load_breast_cancer
from utils.files_utils import save_files

def load_data():
    
    breast_ds = load_breast_cancer()
    labels = np.reshape(breast_ds.target, (569,1))
    breast_data = np.concatenate([breast_ds.data, labels], axis=1)
    df = pd.DataFrame(breast_data)
    print(df.show())
    df.columns = np.append(breast_ds.feature_names, 'label')
    df.name="df"
    print(df.show())
    save_files([df])