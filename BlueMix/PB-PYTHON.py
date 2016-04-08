
# coding: utf-8

# In[35]:

from pyspark.sql import SQLContext
from pyspark.sql.types import *


# In[15]:

get_ipython().magic(u'matplotlib inline')


# In[16]:

sqlContext = SQLContext(sc)


# In[4]:

FOLLCOUNT = sqlContext.read.parquet("FOLLOWERS.PARQUET")


# In[5]:

FOLLCOUNT.registerTempTable("FOLLCOUNT");


# In[20]:

sqlContext.cacheTable("FOLLCOUNT")
Q1 = sqlContext.sql("SELECT * FROM FOLLCOUNT WHERE F_COUNT > 1000000")


# In[21]:

F1 = Q1.toPandas()
F1=F1.set_index('USER_DISPLAY_NAME')


# In[22]:

from pylab import rcParams
import pylab
rcParams['figure.figsize'] = 20,10
Line=F1.plot(kind='line',title='Count by followers of users',stacked=False)
Line.set_ylabel("No.of follwers")
Line.set_xlabel("Users")
pylab.ylim([0,10000000])


# In[23]:

LANGCOUNT = sqlContext.read.parquet("LANG.PARQUET")


# In[24]:

LANGCOUNT.registerTempTable("LANGCOUNT");


# In[28]:

sqlContext.cacheTable("LANGCOUNT")
Q2 = sqlContext.sql("SELECT * FROM LANGCOUNT WHERE L_COUNT > 1000")


# In[29]:

F2 = Q2.toPandas()
F2=F2.set_index('MESSAGE_LANGUAGE')


# In[30]:

from pylab import rcParams
rcParams['figure.figsize'] = 15,5
barh = F2.plot(kind='bar',title='num of tweets in different languages',stacked=False)
barh.set_ylabel("Total num of tweets") 
barh.set_xlabel("language code") 


# In[18]:

COUNTRYCOUNT = sqlContext.read.parquet("COUNTRY1.PARQUET")


# In[19]:

COUNTRYCOUNT.registerTempTable("COUNTRYCOUNT");


# In[20]:

sqlContext.cacheTable("COUNTRYCOUNT")
Q3 = sqlContext.sql("SELECT * FROM COUNTRYCOUNT WHERE MESSAGE_COUNTRY IS NOT NULL")


# In[21]:

F3 = Q3.toPandas()
F3=F3.set_index('MESSAGE_COUNTRY')


# In[22]:

from pylab import rcParams
rcParams['figure.figsize'] = 15,5
Area=F3.plot(kind='area',title='number of tweets in each country',stacked=False)
Area.set_ylabel("Tweets count")
Area.set_xlabel("Country")


# In[98]:

AWARDCOUNT = sqlContext.read.parquet("AW.PARQUET")


# In[99]:

AWARDCOUNT.registerTempTable("AWARDCOUNT");


# sqlContext.cacheTable("AWARDCOUNT")
# Q4 = sqlContext.sql("SELECT * FROM AWARDCOUNT WHERE C2_COUNT < 200")

# In[104]:

sqlContext.cacheTable("AWARDCOUNT")
Q4 = sqlContext.sql("SELECT * FROM AWARDCOUNT ORDER BY C2_COUNT DESC LIMIT 10")


# In[105]:

F4 = Q4.toPandas()
F4=F4.set_index('USER_CITY')


# In[114]:

from pylab import rcParams
rcParams['figure.figsize'] = 15,5
barh = F4.plot(kind='bar',title='NUMBER OF TWEETS IN EACH CITY TALKING ABOUT AWARDS',stacked=False)
barh.set_ylabel("NUMBER OF TWEETS") 
barh.set_xlabel("CITY NAME") 


# In[84]:

SENTICOUNT = sqlContext.read.parquet("SENTIMENT.PARQUET")


# In[85]:

SENTICOUNT.registerTempTable("SENTICOUNT");


# In[86]:

sqlContext.cacheTable("SENTICOUNT")
Q5 = sqlContext.sql("SELECT * FROM SENTICOUNT")


# In[87]:

F5 = Q5.toPandas()
F5=F5.set_index('SENTIMENT_POLARITY')


# In[95]:

from pylab import rcParams
rcParams['figure.figsize'] = 15,5
barh = F5.plot(kind='bar',title='SENTIMENT ANALYSIS',stacked=False)
barh.set_ylabel("Total num of tweets") 
barh.set_xlabel("SENTIMENT") 


# In[89]:

SENTICOUNT2 = sqlContext.read.parquet("SENTIMENT2.PARQUET")


# In[90]:

SENTICOUNT2.registerTempTable("SENTICOUNT2");


# In[91]:

sqlContext.cacheTable("SENTICOUNT2")
Q6 = sqlContext.sql("SELECT * FROM SENTICOUNT2 ORDER BY S2_COUNT DESC LIMIT 20")


# In[92]:

F6 = Q6.toPandas()
F6=F6.set_index('SENTIMENT_TERM')


# In[97]:

from pylab import rcParams
rcParams['figure.figsize'] = 20,10
barh = F6.plot(kind='bar',title='SENTIMENT ANALYSIS',stacked=False)
barh.set_ylabel("Total num of tweets") 
barh.set_xlabel("SENTIMENT") 


# In[66]:

TIMECOUNT= sqlContext.read.parquet("MORNING.PARQUET")
TIMECOUNT2= sqlContext.read.parquet("EVENING.PARQUET")


# In[67]:

TIMECOUNT.registerTempTable("TIMECOUNT");
TIMECOUNT2.registerTempTable("TIMECOUNT2");


# In[68]:

sqlContext.cacheTable("TIMECOUNT")
Q7 = sqlContext.sql("SELECT * FROM TIMECOUNT")
sqlContext.cacheTable("TIMECOUNT2")
Q8 = sqlContext.sql("SELECT * FROM TIMECOUNT2")


# In[81]:

from pylab import rcParams
rcParams['figure.figsize'] = 15, 10


# In[82]:

F7 = Q7.toPandas()
F8 = Q8.toPandas()


# In[83]:

labels = F7['MESSAGE_COUNTRY']
fracs = F7['T_COUNT']

pie(fracs, labels=labels,
                autopct='%1.1f%%', shadow=True,startangle=90)
title('Morning tweets', bbox={'facecolor':'0.8', 'pad':5})

show()
labels2 = F8['MESSAGE_COUNTRY']
fracs2 = F8['T_COUNT']

pie(fracs2, labels=labels2,
                autopct='%1.1f%%', shadow=True, startangle=90)
title('Evening tweets', bbox={'facecolor':'0.8', 'pad':5})

show()


# In[ ]:




# In[ ]:



