# Data Overflow Contest Problem

# Covid Vaccination
The user is given two types of [TSV](https://www.geeksforgeeks.org/simple-ways-to-read-tsv-files-in-python/) files.

* **User Meta file** :  Containing User info  like gender,city and state.
* **Vaccination status files (this can be multiple files)** : Containing Vaccination  information for the user, like the vaccine the user took, date of vaccination in dd-mm-yyyy format.

The goal of this task is to find number of users vaccinated by  city, state,  vaccine or gender.

This result is expected to be saved in a tsv file, below the file structure and format is explained.

## Input File(s)
### Vaccine Status file (Can be multiple)
This is a TSV file containing three columns user, vaccine and date.

Each row(record) tells that user u has taken vaccine v on date d

**User** is of type **Integer**.

**Vaccine** is of type **string**, valid vaccine's are **A**,**B** and **C**, anything other are invalid.

**Date** is  of type **date** in **dd-mm-yyyy** format, it should be between **1st February 2020** to **30th November 2021** (Inclusive).


```
user    vaccine date
9297629 C       29-11-2020
5054692 B       08-07-2021
1348029 A       14-08-2021
8089418 A       03-01-2021
1793913 B       23-08-2020
1423551 B       21-06-2020
6744398 B       16-06-2020
6744397 B       14-06-2020
6893959 A       28-07-2021
7137362 C       29-01-2021
1754454 C       01-01-1970   # This record should be ignored as the date is invalid
9297629 C       03-03-2021
1545454 Z       01-04-2020   # This record should be ignored as the vaccine is invalid
```
### User Meta File
It is a TSV file containing infomation about the user like gender, city and state.

**User** is of type **Integer**.

**gender** is of type **string**, values being M and F.

**City** is of type **string**, you can assume all the cities are valid.

**state** is of type **string**, you can assume all the states are valid


Sample data
```
user    gender  city    state
8089418 F       Bahraigh        Uttar Pradesh
6893959 M       Hyderabad       Telangana
6744398 M       Sonipat Haryana
6744397 M       Sonipat Haryana
1348029 M       Vishakhapatnam  Andhra Pradesh
1793913 M       Bhiwandi        Maharashtra
1423551 F       Raipur  Chhattisgarh
9297629 F       Chennai Tamil Nadu
5054692 F       Chennai Tamil Nadu
7137362 M       Gurgaon Haryana
1754454 M       Mangalore      Karnataka
1545454 F       Chennai Tamil Nadu
```

## Output
The goal of this task is to find number of users vaccinated by  city, state,  vaccine or gender.
The output file is a TSV file containing city, state, vaccine, gender and unique_vaccinated_users

For the above input the output is expected to be

**Note**: The first line should contain headers like below. The order of the columns should be exactly same as shown below.
```
city            state           vaccine    gender   unique_vaccinated_people
Bahraigh        Uttar Pradesh   A          F        1
Bhiwandi        Maharashtra     B          M        1  
Chennai         Tamil Nadu      B          F        1
Chennai         Tamil Nadu      C          F        1
Gurgaon         Haryana         C          M        1 
Hyderabad       Telangana       A          M        1
Raipur          Chhattisgarh    B          F        1
Sonipat         Haryana         B          M        2
Vishakhapatnam  Andhra Pradesh  A          M        1

```

In the above example, The number of **unique** male people who were from Sonipat, Haryana vaccinated with Vaccine **B** is 2. 

## How will your code be tested?
The code will be tested against test cases.

For performance we are testing the code with a file having 100 thousand (1 lakh) records, 1 million records and 10 million records


**Important Note** : While testing your code we will run your code with N Vaccine Status files, 1 <= N <= 10. You can expect all the **N** files to be evenly divided with data.


## Hardware Requirement:

Your code will be run with the following hardware

 **4GB RAM, 4 core CPU**
 
 Please do not feel deterred if your machine is lower in configuration. You can test with what you have
 
## Sample data

We are providing you two sample files

* Sample data containing 100 users can be downloaded from [here](https://dataoverflow.affinityanswers.com/sample_data/problem/100_records.zip), use this for quick testing.
* Sample data containing 1 million users can be downloaded from [here](https://dataoverflow.affinityanswers.com/sample_data/problem/1_million_records.zip), use this for performance testing.

This is a zip file, please uncompress it using a unzip command or simple uncompressing tool.

After uncompressing you should see one files similar to this
```
data
├── user_meta.tsv
└── vaccination_status
    ├── vaccination_status_0.tsv
    ├── vaccination_status_1.tsv
    ├── vaccination_status_2.tsv
    ├── vaccination_status_3.tsv
    ├── vaccination_status_4.tsv
    ├── vaccination_status_5.tsv
    ├── vaccination_status_6.tsv
    ├── vaccination_status_7.tsv
    ├── vaccination_status_8.tsv
    └── vaccination_status_9.tsv
```
 
 * One file named **user_meta.tsv** which is the TSV file containing **user information** like user, gender, city and state.
 * Another folder named **vaccination_status** contains TSV files having vaccination status like the user, the vaccine the user took and finally the date the vaccination happened.  

You can use this sample data to run your code.



## How to get started with the repository?
* Login to github and visit the [repository](https://github.com/affinityanswers/dataoverflow_Dec2021/).
* Fork the repository by clicking the fork button.
![Fork](images/fork.png)
* [Clone](https://git-scm.com/book/en/v2/Git-Basics-Getting-a-Git-Repository#_git_cloning) the forked respository to the local machine.
![Clone](images/clone.png)
* Start writing your code by updating the `covid_vaccine` function in the `code/script.py`  feel free add/modify the code.
* If your code is using additional libraries please mention it in the `requirements.txt`.
* Once you are happy with the code, commit the code
* Submit your github repository link along with the commit id in our [google form](https://docs.google.com/forms/d/e/1FAIpQLSfcLzodcRG4fhD6en4r8o-CqtTXtiwBt_k_28P0G3PXcH9ksg/viewform).


## How to test your code?

Run the basic test cases by running.
  
  ```console
  python3 wrapper.py test
  ```
  
  This tests your code with basic test cases.

## Running your code with your own data files

To run your code with the given sample input file, please run

```console
python3 wrapper.py run -v {vaccination_status_0.tsv} [{vaccination_status_1.tsv} ...]  -u {user_meta.tsv} -o output_file.tsv
```
