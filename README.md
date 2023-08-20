# PySpark Big Data RDD Operations

## Welcome to Project
This project is focused on demonstrating how to use Apache Spark's Resilient Distributed Datasets (RDDs) and perform various transformation and action operations on them. RDDs are a fundamental data structure in Spark that enable parallel and distributed processing of data across a cluster of computers. This project showcases the creation of RDDs, applying transformation functions, and executing action functions on them.

**What is this project ?** 
This project serves as an educational resource for understanding the concepts of RDDs, transformations, and actions in Apache Spark. It provides code examples and explanations of different RDD operations, helping users gain hands-on experience with Spark's capabilities.

**Why did you do it ?** 
This project was created to address the need for a clear and concise educational resource on RDDs in Apache Spark. Many beginners struggle to grasp the concepts of RDDs, transformations, and actions, and this project aims to simplify the learning process by providing step-by-step code examples and explanations.

**What are the gains from doing this project ?**
By working through this project, users can gain a solid understanding of RDDs and how they fit into the larger ecosystem of Apache Spark. Learning about transformation and action operations on RDDs is crucial for performing various data processing tasks efficiently. The project will empower users to harness the power of Spark for big data processing tasks and equip them with valuable skills for data engineering and analysis.

## Architecture Diagram
1. **Create RDD (Resilient Distributed Datasets):** The first step is to create RDDs, which are distributed data structures that can be divided and distributed across the computers in the Spark cluster. RDDs can be created from various sources, such as program variables, other RDDs, or external data sources like files.

2. **Transformation:** After creating RDDs, we can apply Transformations to change the data within the RDDs using functions like map, flatMap, filter, and others. The result of a Transformation is a new RDD with the transformed data.

3. **RDD:** After performing Transformations, we obtain new RDDs that have undergone the specified changes. These RDDs hold distributed data within the Spark cluster.

4. **Actions:** Actions are steps where we actually perform operations on the data within RDDs. This includes actions such as collecting data (collect), counting elements (count), calculating sums (sum), finding maximum values (max), and others.

5. **Results:** The results represent the outcomes of the Actions performed on the data. These results can be numeric values, lists of data, or the results of various computations derived from data processing.

![image](Img/Architecture-Diagra.svg)