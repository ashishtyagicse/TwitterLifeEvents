# TwitterLifeEvents


# Instruction for running in cluster mode with Yarn 
1) Download the code from Github  
2) Change output parquet file path in application.conf file   
3) Change all project relative paths in application.conf from resources/File Name to File Name
4) Change application.conf file path in utils/ConfigProvider.py from resources/application.conf to application.conf  
5) CD to diectory where you unpacked code from github  
6) Create a ZIP for the entire project structure with following command   
`zip -r SocialMediaLifeEvents.zip .`  
7) Create folder on one of the cluster machine with following   
`mkdir SocialMediaLifeEvents`  
8) CD to this folder SocialMediaLifeEvents  
9) Copy SocialMediaLifeEvents.zip , LifeEvents.py and Resources folder from local machine to cluster machines folder SocialMediaLifeEvents  
10) Export the resources folder files path in a variable with following  
`export ResourceFiles=$(find resources/ -name '*.*' ! -name '.DS_Store' | sort | tr '\n' ',' | head -c -1)`
11) Submit spark job with following   
`spark-submit --master yarn --deploy-mode cluster --py-files SocialMediaLifeEvents.zip,$ResourceFiles LifeEvents.py`  
