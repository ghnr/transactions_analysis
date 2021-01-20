from collections import defaultdict
import copy
import csv
from dataclasses import dataclass
from operator import attrgetter


@dataclass
class Transaction:
    """
    Data class that holds the transaction data variables
    """
    # Reduce memory usage of each class instance (also slightly faster attribute access)
    __slots__ = ["transaction_id", "account_id", "transaction_day", "category", "transaction_amount"]
    
    transaction_id: str
    account_id: str
    transaction_day: int
    category: str
    transaction_amount: float

    def __post_init__(self):
        # Force data class types
        try:
            self.transaction_amount = float(self.transaction_amount)
            self.transaction_day = int(self.transaction_day)
        except TypeError as error:
            print(f"Failed to convert types of: {self.transaction_amount} {self.transaction_day} {error}")

    def __lt__(self, other):
        return self.transaction_day < other.transaction_day
        
    def __repr__(self):
        return f"{self.__class__.__name__}({self.transaction_id!r}, {self.account_id!r}, {self.transaction_day}, " \
            f"{self.category!r}, {self.transaction_amount})"
    

class TransactionListAnalysis:
    """
    Analysis class with aggregation methods that operate on the List[Transaction]
    """
    def __init__(self, transactions: list):
        self.transactions = transactions
        self.unique_categories = self.get_unique_categories()
        # Rolling window aggregation variables
        self.rolling_aggregation = None
        self.lower_bound = 0
        self.upper_bound = 0

    def get_daily_totals(self) -> dict:
        """
        Returns: Dictionary of the total transaction amounts (value) by day (key)
        """
        day_totals = defaultdict(int)
    
        for transaction in self.transactions:
            day_totals[transaction.transaction_day] += transaction.transaction_amount
    
        return day_totals
        
    def get_unique_categories(self) -> set:
        """
        Returns: Set of the unique categories that exist in the transactions list
        """
        unique_categories = set()
        
        for transaction in self.transactions:
            unique_categories.add(transaction.category)
        
        return unique_categories
        
    def get_average_by_category(self) -> dict:
        """
        Nested dictionary, each key is an Account ID, and each ID contains a dictionary of category totals and counts
        Returns: Dictionary of category totals and counts by account_id
        """
        # Compute totals and counts while iterating over transactions
        category_totals = defaultdict(lambda: {cat: {"total": 0, "count": 0} for cat in self.unique_categories})
        
        for transaction in self.transactions:
            category_totals[transaction.account_id][transaction.category]["total"] += transaction.transaction_amount
            category_totals[transaction.account_id][transaction.category]["count"] += 1
            
        # Compute averages based on the totals and counts calculated above

        category_averages = defaultdict(lambda: {cat: 0 for cat in self.unique_categories})

        for account_id, category_dict in category_totals.items():
            for unique_category in self.unique_categories:
                total_count_dict = category_dict.get(unique_category, {})
                total = total_count_dict.get("total", 0)
                count = total_count_dict.get("count", 0)
        
                try:
                    average = total / count
                except ZeroDivisionError:
                    average = 0
                    
                category_averages[account_id][unique_category] = average
        
        return category_averages
        
    @staticmethod
    def create_window_default_keys(target_cols: list) -> dict:
        """
        Creates the dictionary that will be used in defaultdict for each Account ID
        e.g. >> {"max": 0, "total": 0, "count": 0, "AA": 0, "CC": 0, "FF": 0}
        where AA, CC, FF are @param target_cols
        Returns: Template dictionary that wil be used in defaultdict
        """
        default_keys = {"max": 0, "total": 0, "count": 0}
    
        for col in target_cols:
            default_keys[col] = 0
    
        return default_keys
    
    def get_rolling_time_window(self, window_size: int = 5, target_total_cols: list = None):
        """
        Performs aggregations over a rolling time window of window_size width. Data is stored in an output dict between
        iterations to return
        Note: Data must be sorted by transaction.transaction_day
        For all days between window_size and end_day:
        {day_num: {account_n: {max: w, total: x, count: y, category_n: z}}}
        e.g. >> {6: {'A1': {'max': 977.98, 'total': 1376.81, 'count': 3, 'AA': 0, 'CC': 171.19, 'FF': 977.98}}}
        Returns: Aggregate output of all rolling time period aggregations
        """
        if target_total_cols is None:
            target_total_cols = ["AA", "CC", "FF"]
        
        # Sort the transactions by day in ascending order, necessary for iterate_window_range()
        self.transactions.sort(key=attrgetter("transaction_day"))

        # +1 to exclude current day
        window_start = window_size + 1
        # List was sorted above so the last item will have the largest day
        window_end = self.transactions[-1].transaction_day

        if (window_size > window_end) or window_size < 2:
            raise ValueError(f"Invalid window size given: {window_size}")
        
        # Uses a default dict for each Account ID access using the template created in create_window_default_keys()
        self.rolling_aggregation = defaultdict(lambda: self.create_window_default_keys(target_total_cols))
        # Outputs need temporary storage to save to file at the end of the window
        aggregation_outputs = {}
        
        # +1 to include the window_end as well in range(start,end)
        for num_day in range(window_start, window_end + 1):
            # start day, end day, cols
            self.iterate_window_range(num_day - window_size, num_day, target_total_cols)
            aggregation_outputs[num_day] = copy.deepcopy(self.rolling_aggregation)
        
        return aggregation_outputs

    def iterate_window_range(self, window_start_day: int, window_end_day: int, target_cols: list):
        """
        Performs efficient iteration over the defined window range.
        The iteration procedure takes advantage of the fact that in a rolling window, only the lower bound is removed
        from the window between iterations and the values between the lower bound and the upper bound remain the same:
        ----------------------------------------
        Example:
        window(n-1)  1, [2, 3, 4, 5, 6], 7, 8...
        window(n)    1, 2, [3, 4, 5, 6, 7], 8...
        
        Values 3,4,5,6 remain inside the window and are unchanged. The lower bound of 2 is removed and a new upper
        bound is added into the window.
        ----------------------------------------
        The transactions list is sliced using the previous lower bound as the start step. The previous lower bound is
        the window_start_day of the previous iteration.
        Any values between the previous lower bound and the current lower bound are "subtracted" from the aggregation.
        Then the iteration skips to the previous upper bound (because the other values are unchanged as above) and
        continues aggregating from there.
        
        @param window_start_day: Rolling transaction window start day (inclusive)
        @param window_end_day: Rolling transaction window end day (not inclusive)
        @param target_cols: Column names of categories to total
        """
        new_lower_bound = None
        
        current_index = 0
        # Was the maximum value changed in the out-of-bounds window removal
        max_changed = False
        
        # Base slice should be created outside of the loop because, even though the list slice is not duplicated in
        # memory, the references _are_ copied and that can be expensive in larger datasets
        transaction_slice = self.transactions[self.lower_bound:]
        
        # While loop used to allow iteration skips using current_index
        while current_index + self.lower_bound < len(self.transactions):
            
            transaction = transaction_slice[current_index]
            
            # Between previous lower bound and new lower bound:
            if transaction.transaction_day < window_start_day:
                # "|=" bitwise "or" operator used so that once max_changed is True, it will remain True
                max_changed |= self.check_max_changed(transaction)
                # Subtract the totals and counts from the out of bounds transactions
                self.update_totals(transaction, target_cols, SUBTRACTING)
                self.update_average(transaction, SUBTRACTING)
            # Between new lower bound and upper bound
            elif window_end_day > transaction.transaction_day >= window_start_day:
                # Set the new lower bound
                if new_lower_bound is None:
                    new_lower_bound = self.lower_bound + current_index
                
                # If maximum did not change, there is no need to re-aggregate the unchanged values, can skip the
                # iteration to the upper bound index. Continue used to avoid index +1 at end of loop.
                if (self.lower_bound + current_index < self.upper_bound) and not max_changed:
                    current_index = self.upper_bound - self.lower_bound
                    continue
                else:
                    # Compute aggregations
                    self.update_max(transaction)
                    self.update_totals(transaction, target_cols, ADDING)
                    self.update_average(transaction, ADDING)
            # Hit the end of the transaction window
            elif transaction.transaction_day >= window_end_day:
                break
            
            # For loop but with index skipping
            current_index += 1

        # Note: Set new upper bound first before changing lower bound
        self.upper_bound = self.lower_bound + current_index
        self.lower_bound = new_lower_bound
        
    def update_max(self, transaction: Transaction):
        """
        Sets a new maximum value if transaction amount > previous maximum
        """
        if transaction.transaction_amount > self.rolling_aggregation[transaction.account_id]["max"]:
            self.rolling_aggregation[transaction.account_id]["max"] = transaction.transaction_amount
    
    def check_max_changed(self, transaction: Transaction) -> bool:
        """
        Returns whether the value being removed is the current maximum transaction amount
        """
        if transaction.transaction_amount == self.rolling_aggregation[transaction.account_id]["max"]:
            self.rolling_aggregation[transaction.account_id]["max"] = 0
            return True
        else:
            return False
    
    def update_totals(self, transaction: Transaction, target_cols: list, operation: int):
        """
        Adds or subtracts the transaction amount (based on @param operation) from the account_id's category
        """
        if transaction.category in target_cols:
            if operation == ADDING:
                self.rolling_aggregation[transaction.account_id][transaction.category] += transaction.transaction_amount
            elif operation == SUBTRACTING:
                self.rolling_aggregation[transaction.account_id][transaction.category] -= transaction.transaction_amount

    def update_average(self, transaction: Transaction, operation: int):
        """
        Adds or subtracts the transaction amount (based on @param operation) and increments the count for each id
        """
        if operation == ADDING:
            self.rolling_aggregation[transaction.account_id]["total"] += transaction.transaction_amount
            self.rolling_aggregation[transaction.account_id]["count"] += 1
        elif operation == SUBTRACTING:
            self.rolling_aggregation[transaction.account_id]["total"] -= transaction.transaction_amount
            self.rolling_aggregation[transaction.account_id]["count"] -= 1
        
    @staticmethod
    def save_daily_totals(filename: str, daily_totals: dict):
        """
        Writes a csv file of [Day, Total] (header) with corresponding values per row
        """
        with open(filename, "w", newline="") as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=',')
            csv_writer.writerow(("Day", "Total"))
            for day, total in daily_totals.items():
                csv_writer.writerow((day, "%.2f" % total))
                
    def save_category_averages(self, filename: str, category_averages: dict):
        """
        Writes a csv file of [Account ID, AA Total, ... , XX Total] (header) for each unique category with
        corresponding values per row
        """
        with open(filename, "w", newline="") as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=',')
            header = ["Account ID"] + [category + " Average" for category in self.unique_categories]
            csv_writer.writerow(header)
        
            for account_id, category_dict in category_averages.items():
                
                csv_row = [account_id]
                
                for unique_category in self.unique_categories:
                    average = category_dict.get(unique_category, 0)
                    csv_row.append("%.2f" % average)
                
                csv_writer.writerow(csv_row)
    
    @staticmethod
    def save_rolling_aggregation(filename: str, rolling_aggregation: dict, target_cols: list):
        """
        Writes a csv file of [Account ID, AA Total, ... , XX Total] (header) for each unique category with
        corresponding values per row
        """
        with open(filename, "w", newline="") as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=',')
            header = ["Day", "Account ID", "Max Transaction", "Mean Transaction"]
            header += [category + " Total Value" for category in target_cols]
            csv_writer.writerow(header)
            
            for day, rolling_obj in rolling_aggregation.items():
                for account_id, aggregation_obj in rolling_obj.items():
                    csv_row = [day, account_id, aggregation_obj["max"]]
                    
                    try:
                        mean_average = "%.2f" % (aggregation_obj["total"] / aggregation_obj["count"])
                    except ZeroDivisionError:
                        mean_average = "0.0"
                    csv_row.append(mean_average)
                    
                    for category in target_cols:
                        csv_row.append("%.2f" % aggregation_obj[category])
                    
                    csv_writer.writerow(csv_row)

            
if __name__ == "__main__":
    DATA_PATH = "transactions.txt"
    # Arbitrary operation constants used for aggregation addition/subtraction
    SUBTRACTING = 0
    ADDING = 1
    # Default category names to total in the rolling window aggregation
    TARGET_COLS = ["AA", "CC", "FF"]
    
    transactions_list = []
    
    with open(DATA_PATH, "r") as data_file:
        csv_reader = csv.reader(data_file)
        next(csv_reader, None)
        for row in csv_reader:
            # *Unpack row values into Transaction() arguments
            transactions_list.append(Transaction(*row))
    
    analysis = TransactionListAnalysis(transactions_list)
    analysis.save_daily_totals("daily_totals.csv", analysis.get_daily_totals())
    analysis.save_category_averages("category_averages.csv", analysis.get_average_by_category())
    analysis.save_rolling_aggregation("rolling_time_window.csv", analysis.get_rolling_time_window(5), TARGET_COLS)
