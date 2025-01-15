# Text cleaning
import re
import spacy
import nltk
from nltk.corpus import stopwords
import contractions
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

nlp = spacy.load('en_core_web_sm')
nltk.download('stopwords')

def clean_text(text):
    # convert contractions like 'I'm -> I am'
    text = contractions.fix(text)
    text = text = re.sub('[^A-Za-z]+', ' ', text)
    return text

def get_stopwords():
    for sw in stopwords.words('english'):
        nlp.Defaults.stop_words.add(sw)
        nlp.vocab[sw].is_stop = True
    return nlp

def lemmatize(text):
    doc = nlp(text)
    return [token.lemma_ for token in doc]

def remove_stopwords(tokens):
    return " ".join(str(token) for token in tokens if(nlp.vocab[token].is_stop == False))

def preprocess(text, do_lemmatize=True):
    get_stopwords()
    text = clean_text(text)
    tokens = lemmatize(text)
    tokens = remove_stopwords(tokens)
    return tokens

def extract_sentiment():
    analyzer = SentimentIntensityAnalyzer()