import pickle

from processor import DataPreProcessor
from sklearn.pipeline import Pipeline
from sklearn.linear_model import SGDClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report as clsr
from sklearn.cross_validation import train_test_split as tts

def identity(arg):
    return arg

def build_and_save_model(X, y, filepath):
    """
    This function does the following:
    - Build a classifier (SGD)
    - Fit our data to the classifier
    - Run cross validation to test the accuracy of our model
    """
    def build(classifier, X, y=None):
        """
        Build a model based on our process, a vectorizer and a linear classifier
        """
        if isinstance(classifier, type):
            classifier = classifier()

        model = Pipeline([
            ('preprocessor', DataPreProcessor()),
            ('vectorizer', TfidfVectorizer(tokenizer=identity, preprocessor=None, lowercase=False)),
            ('classifier', classifier),
        ])

        model.fit(X, y) # Fit the model to our data
        return model

    # Label encode the classes we chose
    labels = LabelEncoder()
    y = labels.fit_transform(y)

    # Split data into train/test
    X_train, X_test, y_train, y_test = tts(X, y, test_size=0.1)
    model = build(SGDClassifier, X_train, y_train)

    # Predict the results of test data and calculate accuracy
    y_pred = model.predict(X_test)
    print(clsr(y_test, y_pred, target_names=labels.classes_))

    model.labels_ = labels

    with open(filepath, 'wb') as f:
        pickle.dump(model, f)

    return model
