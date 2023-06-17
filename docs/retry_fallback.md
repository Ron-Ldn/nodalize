# Calculation retry

The *DataNode* class exposes 2 properties to help drive the retry mechanism:
- *retry_wait*
- *retry_timeout*

In case of failure during the compute, the *Coordinator* will use these parameters to decide wether or not to retry and for how long.

Using the example below, a failure would cause the *Coordinator* to wait for 1 minutes before retrying. In case of multiple failures, the *Coordinator" would stop trying after 10 minutes.

```python
class MyNode(DataNode):
    @property
    def retry_wait(self) -> int:
        """
        Get time to wait, in seconds, between each tries to generate the data.

        Returns:
            time on seconds
        """
        return 60  # default is 0

    @property
    def retry_timeout(self) -> int:
        """
        Get time window, in second, where the data generation can be retried.

        0 means no retry

        Returns:
            time on seconds
        """
        return 600  # default is 0
```

The default values for both properties is 0. 0 for *retry_timeout" means that the *Coordinator" will stop afte the first failure.

# Fallback

The *DataNode* class exploses 2 callback methods in case of failure:
- *on_compute_error"
- *on_failure*

*on_compute_error* will be called each time the *compute" method is raising an exception. In case of multiple retries, the callback will be invoked for each error.

*on_failure* will be called only once after the *Coordinator* stopped trying.

```python
class MyNode(DataNode):
    def on_compute_error(self) -> None:
        """Perform custom action when the computation of the node failed."""
        pass

    def on_failure(self) -> None:
        """Perform custom action when data generation failed."""
        pass
```

# Illustration

Let's analyze the following example. The *MyNode* class will systematically fail. After each failure, the *Coordinator* will log "I tried" and then wait for 5 seconds. Once 50 seconds have past since before the 1st call to *compute*, the *Coordinator* will stop trying, will log "I failed" and wil raise a new exception.

```python
import sys
import logging

from nodalize.orchestration.coordinator import Coordinator
from nodalize.data_management.sqlite_data_manager import SqliteDataManager

from nodalize.datanode import DataNode


root = logging.getLogger()
root.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s: %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)


class MyNode(DataNode):
    @property
    def calculator_type(self):
        return "pandas"

    def compute(self, parameters):
        raise AssertionError("Unexpected failure")
    
    @property
    def retry_wait(self):
        return 5

    @property
    def retry_timeout(self):
        return 50

    def on_compute_error(self):
        logging.error("I tried")

    def on_failure(self):
        logging.error("I failed")


coordinator = Coordinator("MyApp")
coordinator.set_data_manager("sqlite", SqliteDataManager("TestDb/MyDb"), default=True)
coordinator.create_data_node(MyNode)
coordinator.set_up()

coordinator.compute_and_save(MyNode.__name__)
```

This code will produce an output similar to this:

```
2023-06-17 15:15:19,072: Batch id generated: 1687014919072403
2023-06-17 15:15:19,072: Starting computation of data node: MyNode
2023-06-17 15:15:19,072: Started at 1687011319.0729797, time out at 1687011369.0729797
2023-06-17 15:15:19,073: Failed to compute MyNode
Traceback (most recent call last):
  File "../nodalize/orchestration/coordinator.py", line 186, in compute_and_save_single_node
    data = node.load_and_compute(parameters, new_batch_id, parent_batch_ids)
  File "../nodalize/datanode.py", line 362, in load_and_compute
    data = self.compute(parameters, **dependency_loaders)
  File "..", line 27, in compute
    raise AssertionError("Unexpected failure")
AssertionError: Unexpected failure
2023-06-17 15:15:19,073: I tried
2023-06-17 15:15:19,074: Next try scheduled to start at 1687011324.0740101
2023-06-17 15:15:19,074: Going to sleep for 5 seconds
2023-06-17 15:15:24,079: Will retry MyNode now
2023-06-17 15:15:24,079: Failed to compute MyNode
Traceback (most recent call last):
  File "../nodalize/orchestration/coordinator.py", line 186, in compute_and_save_single_node
    data = node.load_and_compute(parameters, new_batch_id, parent_batch_ids)
  File "../nodalize/datanode.py", line 362, in load_and_compute
    data = self.compute(parameters, **dependency_loaders)
  File "..., line 27, in compute
    raise AssertionError("Unexpected failure")
AssertionError: Unexpected failure
2023-06-17 15:15:24,080: I tried
2023-06-17 15:15:24,080: Next try scheduled to start at 1687011329.0802736
2023-06-17 15:15:24,080: Going to sleep for 5 seconds
2023-06-17 15:15:29,086: Will retry MyNode now
2023-06-17 15:15:29,087: Failed to compute MyNode
Traceback (most recent call last):
  File "../nodalize/orchestration/coordinator.py", line 186, in compute_and_save_single_node
    data = node.load_and_compute(parameters, new_batch_id, parent_batch_ids)
  File "../nodalize/datanode.py", line 362, in load_and_compute
    data = self.compute(parameters, **dependency_loaders)
  File "...", line 27, in compute
    raise AssertionError("Unexpected failure")
AssertionError: Unexpected failure
2023-06-17 15:15:29,088: I tried
2023-06-17 15:15:29,088: Next try scheduled to start at 1687011334.088438
2023-06-17 15:15:29,088: Going to sleep for 5 seconds
2023-06-17 15:15:34,094: Will retry MyNode now
2023-06-17 15:15:34,094: Failed to compute MyNode
Traceback (most recent call last):
  File "../nodalize/orchestration/coordinator.py", line 186, in compute_and_save_single_node
    data = node.load_and_compute(parameters, new_batch_id, parent_batch_ids)
  File "../nodalize/datanode.py", line 362, in load_and_compute
    data = self.compute(parameters, **dependency_loaders)
  File "...", line 27, in compute
    raise AssertionError("Unexpected failure")
AssertionError: Unexpected failure
2023-06-17 15:15:34,094: I tried
2023-06-17 15:15:34,094: Next try scheduled to start at 1687011339.0948746
2023-06-17 15:15:34,094: Going to sleep for 5 seconds
2023-06-17 15:15:39,100: Will retry MyNode now
2023-06-17 15:15:39,101: Failed to compute MyNode
Traceback (most recent call last):
  File "../nodalize/orchestration/coordinator.py", line 186, in compute_and_save_single_node
    data = node.load_and_compute(parameters, new_batch_id, parent_batch_ids)
  File "../nodalize/datanode.py", line 362, in load_and_compute
    data = self.compute(parameters, **dependency_loaders)
  File "...", line 27, in compute
    raise AssertionError("Unexpected failure")
AssertionError: Unexpected failure
2023-06-17 15:15:39,102: I tried
2023-06-17 15:15:39,102: Next try scheduled to start at 1687011344.1025105
2023-06-17 15:15:39,102: Going to sleep for 5 seconds
2023-06-17 15:15:44,109: Will retry MyNode now
2023-06-17 15:15:44,109: Failed to compute MyNode
Traceback (most recent call last):
  File "../nodalize/orchestration/coordinator.py", line 186, in compute_and_save_single_node
    data = node.load_and_compute(parameters, new_batch_id, parent_batch_ids)
  File "../nodalize/datanode.py", line 362, in load_and_compute
    data = self.compute(parameters, **dependency_loaders)
  File "...", line 27, in compute
    raise AssertionError("Unexpected failure")
AssertionError: Unexpected failure
2023-06-17 15:15:44,110: I tried
2023-06-17 15:15:44,111: Next try scheduled to start at 1687011349.111321
2023-06-17 15:15:44,111: Going to sleep for 5 seconds
2023-06-17 15:15:49,117: Will retry MyNode now
2023-06-17 15:15:49,118: Failed to compute MyNode
Traceback (most recent call last):
  File "../nodalize/orchestration/coordinator.py", line 186, in compute_and_save_single_node
    data = node.load_and_compute(parameters, new_batch_id, parent_batch_ids)
  File "../nodalize/datanode.py", line 362, in load_and_compute
    data = self.compute(parameters, **dependency_loaders)
  File "...", line 27, in compute
    raise AssertionError("Unexpected failure")
AssertionError: Unexpected failure
2023-06-17 15:15:49,119: I tried
2023-06-17 15:15:49,119: Next try scheduled to start at 1687011354.1198537
2023-06-17 15:15:49,120: Going to sleep for 5 seconds
2023-06-17 15:15:54,126: Will retry MyNode now
2023-06-17 15:15:54,126: Failed to compute MyNode
Traceback (most recent call last):
  File "../nodalize/orchestration/coordinator.py", line 186, in compute_and_save_single_node
    data = node.load_and_compute(parameters, new_batch_id, parent_batch_ids)
  File "../nodalize/datanode.py", line 362, in load_and_compute
    data = self.compute(parameters, **dependency_loaders)
  File "...", line 27, in compute
    raise AssertionError("Unexpected failure")
AssertionError: Unexpected failure
2023-06-17 15:15:54,128: I tried
2023-06-17 15:15:54,128: Next try scheduled to start at 1687011359.128406
2023-06-17 15:15:54,128: Going to sleep for 5 seconds
2023-06-17 15:15:59,134: Will retry MyNode now
2023-06-17 15:15:59,135: Failed to compute MyNode
Traceback (most recent call last):
  File "../nodalize/orchestration/coordinator.py", line 186, in compute_and_save_single_node
    data = node.load_and_compute(parameters, new_batch_id, parent_batch_ids)
  File "../nodalize/datanode.py", line 362, in load_and_compute
    data = self.compute(parameters, **dependency_loaders)
  File "...", line 27, in compute
    raise AssertionError("Unexpected failure")
AssertionError: Unexpected failure
2023-06-17 15:15:59,136: I tried
2023-06-17 15:15:59,137: Next try scheduled to start at 1687011364.1369653
2023-06-17 15:15:59,137: Going to sleep for 5 seconds
2023-06-17 15:16:04,143: Will retry MyNode now
2023-06-17 15:16:04,143: Failed to compute MyNode
Traceback (most recent call last):
  File "../nodalize/orchestration/coordinator.py", line 186, in compute_and_save_single_node
    data = node.load_and_compute(parameters, new_batch_id, parent_batch_ids)
  File "../nodalize/datanode.py", line 362, in load_and_compute
    data = self.compute(parameters, **dependency_loaders)
  File "...", line 27, in compute
    raise AssertionError("Unexpected failure")
AssertionError: Unexpected failure
2023-06-17 15:16:04,145: I tried
2023-06-17 15:16:04,145: Time out reached for {node.identifier}
2023-06-17 15:16:04,145: I failed
Traceback (most recent call last):
  <callstack>
AssertionError: Failed to generate MyNode: Unexpected failure
```