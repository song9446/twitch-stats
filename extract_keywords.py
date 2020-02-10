import re
from datetime import datetime
try:
    import yake
    from stopwords import STOP_WORDS
    ke = yake.KeywordExtractor(n=1, top=100, windowsSize=1)
    ke.stopword_set = STOP_WORDS
except Exception as e:
    raise(e)
#ke = pke.unsupervised.TfIdf()
#ke = pke.unsupervised.TopicRank()

def normalize(t):
    tail = t.find("- ")
    if tail >= 0:
        t = t[:tail]
    tail = t.find(" -")
    if tail >= 0:
        t = t[:tail]
    if "dc App" in t: print(tail, t)
    t = " ".join(t.split())
    t = re.sub('\.{3,}', '..', t)
    t = re.sub('[ㅜㅠ]{2,}', 'ㅜㅜ', t)
    t = re.sub('[w]{3,}', 'ww', t)
    t = re.sub('[ㄱ]{3,}', 'ㄱㄱ', t)
    t = re.sub('[ㄴ]{3,}', 'ㄴㄴ', t)
    t = re.sub('[ㅡ]{3,}', 'ㅡ', t)
    t = re.sub('[?]{3,}', '??', t)
    t = re.sub('[!]{3,}', '!!', t)
    t = re.sub('[ㅅ]{3,}', 'ㅅㅅ', t)
    t = re.sub('[ㅋㄱㅎz]{2,}', 'ㅋㅋ', t)
    t = re.sub('[ㅇ]{3,}', 'ㅇㅇ', t)
    #t = ''.join(c for i,c in enumerate(t) if i<3 or not (t[i-3] == t[i-2] == t[i-1] == t[0])).split()
    t = list(dict.fromkeys(t.split()).keys())
    return " ".join(t)

def extract_keywords(texts):
    texts = [normalize(i) for i in texts]
    texts = list(dict.fromkeys(texts).keys())
    keywords = ke.extract_keywords("\n".join(texts))
    keywords = [(k,1/v) for k,v in keywords]
    vsum = sum(i for _,i in keywords)
    keywords = [(k,max(v/vsum, 0)*100) for k,v in keywords]
    sum_val = sum(v for k,v in keywords)
    return keywords

if __name__ == "__main__":
    print(normalize("ㅋㅋㅋㅋㅋㅋㅋ ㅋㅋ 말이되나 이게ㅋㅋㅋ"))
    #print(extract_keywords(["가지마 가지마 가지마ㅋㅋ 가지마~~~~ 아아아아~~~지이익~~ 너~~를 위 ~~~~해~~~"]))
