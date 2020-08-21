### Summary
<p align = "justify"> 
This project focuses on delivering a reliable mortality prediction model for patients in the intensive care unit (ICU) using a combination of structured and latent topic features. The meaningful topics relevant to each clinical note are extracted from free-text hospital notes using Latent Dirichlet Allocation model. Several binary classifiers, such as Logistic Regression (LR), Linear Support Vector Machines (SVM), Random Forest (RF), and Gradient Boosted Trees (GBT), are implemented using Scala for prediction task, enabling comparative performance evaluation that was not a part of the seminal work, which only utilized the Linear SVM classifier. The final models are evaluated through the area under the receiver operating characteristic curve (AUROC) and area under precisionrecall curve (AUPR) using both training and testing data derived from the ICU data in the MIMIC-III database.
</p> 

<p align = "center">
	<img width="600" src = "https://github.com/rojinnew/mortality_prediction/blob/master/image.png">
</p>

### Running

Step #1: Preprocessing with Python (64-bit version)

- Copy NOTEEVENTS.csv from MIT MIMIC Code Repository into mimicCode6250/preprocessNote folder
 
- Run the python script with the following command => python2 preprocess.py

- Copy the output file called new2.csv into a dataset folder at following location mimicCode6250/code/dataset/

- Copy NOTEEVENTS.csv, ADMISSIONS.csv, CHARTEVENTS.csv, DIAGNOSES_ICD.csv, ICUSTAYS.csv, INPUTEVENTS_CV.csv, INPUTEVENTS_MV.csv, LABEVENTS.csv, OUTPUTEVENTS.csv, PATIENTS.csv, PROCEDUREEVENTS_MV.csv, SERVICES.csv from MIT  MIMIC Code Repository into dataset folder at following location mimicCode6250/code/dataset/

Step #2: Calculating Severity Scores

NOTE: The outputs from the following codes in this step are also provided in the severityScores subfolder of the dataset folder. Copying them up from the severityScores subfolder to the dataset folder if you wish to skip this step.

- Install IntelliJ IDEA for Spark Scala

- Import Project by selecting the code folder at following location mimicCode6250/code/ and choosing the sbt as external model on the next screen

- Wait until the code has synced successfully (i.e., "dump project structure from sbt"  and "import to IntelliJ project model" are successfully completed) 

- Select the "Run" menu and choose "Edit Configuration" and set the following VM options as defaults: -Xms512M -Xmx64G -Xss1G -XX:+CMSClassUnloadingEnabled

- Run Score.scala inside main folder at the following location mimicCode6250/code/main/scala/edu/gatech/cse6250/main/Score.scala

- Run APSIII.scala inside main folder at the following location mimicCode6250/code/main/scala/edu/gatech/cse6250/main/APSIII.scala

- After running APSIII.scala and Score.scala, the following subfolders are produced as outputs in the dataset folder: sapsii-folder, sofa-folder, oasis-folder, mergedaspii

Step #3: Creating vector topic and structured features and producing the labels 

NOTE: AllNote.scala reads the severity scores from the hardcoded CSV filenames that are provided with the zipfile. If the calculations in step #2 were carried out, they will likely produce CSV files with different names. You will need to change the CSV filenames in the AllNote.scala accordingly. The lines that read these CSV files are: lines #142, #160, #177, and #192. 

- Create an empty text file called output.txt in the code folder as follows: mimicCode6250/code/output.txt

- In IntelliJ, select run => Edit Configuration => Logs => Save console output to file and choose mimicCode6250/code/output.txt as the output file.

- Run AllNote.scala. The results from LDA as well as AUROC, AUROP, Precision, Recall, Specificity, and Sensitivity values from testing and training runs of all classifiers will be printed and saved in this output text file. 

- All (24) ROC plots can also be found in the code folder when the execution is completed.

### Refrences 

MIT Laboratory for Computational Physiology. MIMIC Code Repository: MIMIC-III database. 2018.
https://mimic.physionet.org.
