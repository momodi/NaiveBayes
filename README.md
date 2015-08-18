# NaiveBayes
NaiveBayes on Spark

First run this command to prepare the model.

    sh NaiveBayes.sh compile  && sh NaiveBayes.sh bayes_stat
  
Second to predict:

    sh NaiveBayes.sh compile  && sh NaiveBayes.sh bayes_predict
