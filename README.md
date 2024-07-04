# prepare_ecotaxa_dataset

This code helps to prepare a dataset for machine learning purposes, from one or several EcoTaxa projects.

The steps are:

1. download the metadata (object id, features, etc.) from EcoTaxa
2. download the corresponding images from EcoTaxa
3. remap the taxonomy, to (optionally) regroup detailed taxa into larger units suitable for machine learning
4. re-process the images from EcoTaxa with scikit-image to extract new features, consistent across datasets
5. package the datasets.

The result can be uploaded to SeaNoe. One example of such a dataset is https://www.seanoe.org/data/00446/55741/


