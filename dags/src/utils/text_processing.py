# Text cleaning
import os, re, spacy, nltk, contractions
from nltk.corpus import stopwords
from textblob import TextBlob

class SentimentPipeline:

    def __init__(self):
        self._nlp = spacy.load('en_core_web_sm')
        nltk.download('stopwords')

    def _clean_text(self,text):
        # convert contractions like 'I'm -> I am'
        text = contractions.fix(text)
        text = text = re.sub('[^A-Za-z]+', ' ', text)
        return text    

    def _get_stopwords(self, stowords):
        for sw in stopwords.words('english'):
            self._nlp.Defaults.stop_words.add(sw)
            self._nlp.vocab[sw].is_stop = True

    def _lemmatize(self, text):
        doc = self._nlp(text)
        return [token.lemma_ for token in doc]

    def _remove_stopwords(self, tokens):
        return " ".join(str(token) for token in tokens if(self._nlp.vocab[token].is_stop == False))

    def _preprocess(self, text):
        self._get_stopwords(stopwords)
        text = self._clean_text(text)
        tokens = self._lemmatize(text)
        tokens = self._remove_stopwords(tokens)
        return tokens

    def get_polarity_score(self, text):
        clean_txt = self._preprocess(text)
        testimonial = TextBlob(text)
        return testimonial.sentiment.polarity


