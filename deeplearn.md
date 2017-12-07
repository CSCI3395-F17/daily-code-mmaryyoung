I tried different configurations and it turned out around batch size 500 performs optimally.

batch size: 500 training ratio: 0.65
Number of classes:|5
:-----:|:-----:
Accuracy:|0.5143
Precision:|0.5261	(2 classes excluded from average)
Recall:|0.5778	(2 classes excluded from average)
F1 Score:|0.5225	(2 classes excluded from average)

batch size: 500 training ratio: 0.80
Number of classes:|5
:-----:|:-----:
Accuracy:|0.5100
Precision:|0.5319	(2 classes excluded from average)
Recall:|0.3283
F1 Score:|0.5366	(2 classes excluded from average)

batch size: 500 training ratio: 0.65
Number of classes:|2
:-----:|:-----:
Accuracy:|0.8229
Precision:|0.7467
Recall:|0.6777
F1 Score:|0.8920

batch size: 500 training ratio: 0.80
Number of classes:|2
:-----:|:-----:
Accuracy:|0.8300
Precision:|0.7060
Recall:|0.6579
F1 Score:|0.8994
