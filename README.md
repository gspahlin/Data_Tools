# Data_Tools
<br><br>
This file contains functions for data analysis and database manipulation that I have developed over time. While some of these had very specific applications in the projects I've worked on, these have been selected because they have a potential for wider application. 
In order to use this module you will need several packages in your virtual environment. 
<br><br>
<b>Virtual Environment</b>
<br>
To install the virtual environment run the following commands. The name "data" is recommended for this environment, but any name may be used. Run the following commands in an anaconda prompt window:
<br>
<blockquote>
 $conda create -n data<br>
 $conda activate data<br>
 $pip install numpy pandas pyarrow matplotlib seaborn SQLAlchemy cx-Oracle<br>
 $pip install xlwings==0.23.0</blockquote>
This module is designed for importation into a Jupyter Notebook or similar Python notebook interface. If you do not know how to add a virtual environment to your notebook program, make sure these packages are added to your base environment. I'm using cx-Oracle
for interfacing with an Oracle database. If you want to use these functions for a different database management system such as PostgreSQL, you will need a different driver. 
A guide for adding a virtual environment to a Jupyter Notebook can be found <a href="https://medium.com/@nrk25693/how-to-add-your-conda-environment-to-your-jupyter-notebook-in-just-4-steps-abeab8b8d084">in this article on Medium</a>.
<br><br>
Note: xlwings is a package for scripting events within MS Excel using Python. Versions of excel with python enabled may produce conflicts, the resolution of which is beyond the scope of this readme. 
<br><br>
<b>Importing The Environment</b>
<br>
Import individual classes from data tools using the following syntax
<br>
<blockquote>Import Sort_tools from sort_tools_v2 as gsst </blockquote>
Then individual methods can be called from the module with the aliased prefix
<br>
<blockquote>interval_df=gsst.pt_date_interval_v4(example_df, 'id', 'scan_dates')</blockquote>
<br>
<b>Classes within Data_Tools</b>
<br>
Sort_tools: This is a collection of functions that can be used for common data manipulations and to create specialized pandas dataframes. 
<br>
Data_construct: This is a class designed to read and organize a folder full of data files for cases where you have multiple files to contend with. It can be imported and run with the following syntax:
<br>
<blockquote>import Data_construct from sort_tools_v2
 path='path/to/folder'
 dataset=Data_construct(path)
 dataset.run()</blockquote>
The resulting dfs can be called using keys derived from the last component of their name after an underscore. So if exampdle_dates.csv exists in your data folder, it can be accessed like this:
<blockquote> dataset.dfs['dates']</blockquote>
Db_tools: contains some methods for interacting with an Oracle database. 
<br><br>
Archive: This contains functions I've written that have been improved upon, functions that I seldom use, or functions with signficant flaws. I've kept these around for reference purposes, and do not recommend using these.
In some cases the doc string on these methods will tell you why they have been archived.





