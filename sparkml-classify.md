| Classifier            | Multiclass Accuracy | Binary Accuracy   |
|-----------------------|---------------------|-------------------|
| Random Forrest        | 0.49                | 0.861             |
| Gradient Boosted Tree | -Binary Only-       | 0.844             |
| Multilayer Perceptron | 0.006               | N/A               |
| Linear SVM            | -Binary Only-       | 0.846             |
| One Vs Rest(with GBT) | 0.40                | -Multiclass Only- |
| Naive Bayes           | 0.22                | 0.498             |

The most optimal classifer is random forrest for both multiclass and binary classification. The key feature for random forrest classifier is that it is composed of many randomly initiated decision trees.  
