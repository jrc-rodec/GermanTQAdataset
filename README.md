# GermanTQAdataset
German Language Dataset and Annotation Tool for Tabular Question Answering
## Dataset

The dataset/ folder contains the following CSV files:

* __training.csv__ – Full annotated dataset with question-answer (Q&A) pairs and metadata.
* __training_sustainability.csv__ – Subset of training.csv with only Q&A pairs related to sustainability.
* __test_data.csv__ – 20% split from training_sustainability.csv, used exclusively for testing (not used during fine-tuning of TAPASGO).
* __tables/__ – Contains 360 CSV tables used for question answering:
  The first 236 tables (file names starting with numbers) are general governmental data.
  The remaining 124 tables are from sustainability reports of three major banking institutions in Austria.

## UI Data Collector

The annoatation tool consists of two subprojects: 
* __backend__ that is responsible for the data processing
* __frontend__ where a user can input Q&A pairs for a given table row
 The subprojects can be found inside the annoatation_tool folder.
