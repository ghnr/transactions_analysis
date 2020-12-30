# Transactions Data Analysis

#### Design decisions in the pure Python solution

- Usage of slots: A `Transaction` data class is used to represent each transactional record in the dataset using the unpack (*) operator on the arguments read from the csv file. If there are a large number of transactions, there is some class instantiation overhead introduced by Python as it tries to dynamically allocate the instance attributes. As the instance attributes of the data class are fixed, slots can be used to reserve those attributes instead and store them in "slots" instead of a dictionary. The practical consequence of this is a dramatic reduction in memory usage per class instance.
- Usage of floats: As the transaction dataset is a record of real-world monetary transactions, values with decimal places are to be expected in the transaction amount column. There is a built-in Python library called `decimal` that supports working with decimal numbers without introducing floating point errors incurred as a result of float's naturally internal binary representation. However, `decimal` uses a software implementation instead of the CPU's floating point registers, as such, there is a performance penalty which some online tests claim to be as high as 30%. It was only 5% slower during my tests but it is still a penalty paid for unnecessary accuracy gained when dealing with a task such as meta-analysis of large transaction data. If exact accuracy is imperative for another task, then the cost-benefit analysis of this design should be re-evaluated.  
- Rolling window algorithm: When dealing with rolling windows, the data iteration process can be optimised by using the fact that, between iterations, the majority of a rolling window's data remains unchanged. This is the premise for the algorithm used in the `iterate_window_range` function. As the dataset is iterated over, it records the previous lower bound of the data and only subtracts data between the previous lower bound and the current lower bound, as that data is now not part of the rolling window. Then, the iteration index can be skipped to the end of the window as the data sitting in the middle is unchanged, saving precious iteration and aggregation cycles. There is an example provided in the function docstring.