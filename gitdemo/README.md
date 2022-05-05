# Git Repos in Database
This repository is to demostrate Git Repos integration with in Databricks

## Case 1 - Clone Repository
* Open Databricks Workspace
* Select **Repos** -> **User Name** -> **Add Repo**

	![image](https://user-images.githubusercontent.com/95003669/166628300-216985f8-8ce2-4015-b4a2-f74d8e38d00c.png)

* Provide Git Repo URL **https://github.com/gurpreetseth-db/gitdemo**, Repo Name will automatically gets populated, if you want you can change the name as this is your local copy. 

	![image](https://user-images.githubusercontent.com/95003669/166628041-57e05ccd-66f5-408d-a3b2-6f01ce5bf068.png)

* Hit **Create**
* Once done, will be able to see **gitdemo** folder created with a **README.md** file in it

	![image](https://user-images.githubusercontent.com/95003669/166628670-03019e6c-4152-4215-93ea-088689f5e98e.png)


##
## Case 2 - Create a New File In Databricks and Push it to Git
* Click on drop down arrow **gitdemo** repo (next to main branch logo)
* Select **Create** -> **Notebook** with below values
	* Name : Test_Git
	* Language : Python
	* Cluster : <<Attach to any running cluster>>
	
	![image](https://user-images.githubusercontent.com/95003669/166629028-8f14bd74-6719-4379-8c2f-ecdb8d9f9aee.png)
	
	![image](https://user-images.githubusercontent.com/95003669/166629682-435b7c9c-6a38-4b02-b1e0-c48240ec7159.png)

* Type any valid Python command in the cell/cells
	
	![image](https://user-images.githubusercontent.com/95003669/166629912-84f8fbef-d848-4884-9eb1-5431f1ec33bd.png)

* Click on drop down arrow **gitdemo** repo (next to main branch logo)
* Click on **Git**
	
	![image](https://user-images.githubusercontent.com/95003669/166630092-9f49d358-b3da-43a7-a6b1-f1afa89b236f.png)

* Provide **Summary** & **Description (Optional)** and click **Commit & Push**

	![image](https://user-images.githubusercontent.com/95003669/166630938-7668c272-eeb6-4691-b2c2-978df97f0af7.png)

* With this, we have **Created**, **Pushed** our first file from Databricks workspace to Github
	
##	
## Case 3 - Handle Conflicts
* Lets demonstrate as how t handle conflicts in Databricks Notebooks
* In below screenshot, we have opened **Test_Git.py** file in **EDIT** mode with in **GIT** and changed content for **Row 3** from **Entigeration** to **Integration** (We initially purposly did this spelling mistake) and saved it.
	
	<img width="1518" alt="image" src="https://user-images.githubusercontent.com/95003669/166634584-c9f2ea4b-664c-4844-87ac-2e30f23db1f6.png">

	<img width="1518" alt="image" src="https://user-images.githubusercontent.com/95003669/166634922-16f3cea4-b84c-4247-a74b-51a7f13b501d.png">

* In Databricks Notebook, make change to **Test_Git.py**. Example, in below screenshot, we added a new command. 
	##Note - in Cmd1, we can still see spelling mistake i.e. Entigeration 
	
	![image](https://user-images.githubusercontent.com/95003669/166636259-4509f030-1850-4ae6-b338-f84a116ff6d1.png)

*  Click on drop down arrow **gitdemo** repo (next to main branch logo) and select **Git** 
	
	![image](https://user-images.githubusercontent.com/95003669/166636752-2a4105b2-7119-415d-b89c-7a71a903765d.png)
 
* We will see a screen as per below screenshot where it will show change details that we performed. Provide Summary and hit **Commit & Push** 
	
	![image](https://user-images.githubusercontent.com/95003669/166638043-9c1e998b-fbef-4578-918a-4f6a35e24eeb.png)
	
* What we saw...... An Error, its a conflict as the same file has already been modified by us in GIT and that change is not present in our local Databricks Environment. 
	
	![image](https://user-images.githubusercontent.com/95003669/166638299-a9f9fe66-787e-46ab-b93d-94f3f6515dc2.png)


* As the message suggests, lets **Pull** changes and see what will happen? 

* We get a message, that an pull request will clear results of the notebook (which should be fine). Hit **Confirm**
	
	![image](https://user-images.githubusercontent.com/95003669/166638523-69e0ab51-eaa3-4712-8964-3c5dc86c2e3a.png)

* What we saw....... An Error, This time error about a **Merge Conflict** 
	
	![image](https://user-images.githubusercontent.com/95003669/166638750-405265e9-1daf-4922-a1e1-69bba26bdc02.png)

	
* So, what the options that we have to resolve this **Merge Conflict**?
	* Cancel our operation and discard our changes - which means loss of our work
	* Resolve conflict using a Pull request from a new Branch - This means create a new branch for your work, pull chages and then merge changes from newly created branch to main. **We will use this 2nd option**
	
* Click on **Resolve Conflict using PR** button (as seen in previous screenshot above). it will open a new window where we can specify **New Branch Name** and **Commit Message** and click on **Commit to new branch**
	
	<img width="1430" alt="image" src="https://user-images.githubusercontent.com/95003669/166838364-2dba627a-bd49-4315-9545-92723e39c696.png">
	
* Once done, we will see that default branch is new branch that we have created, click on **Pull** to fetch changes in this new branch
	
	![image](https://user-images.githubusercontent.com/95003669/166838698-df2df21c-a949-4a2b-b446-653346796dde.png)
	
* Now we can **Cherry Pick** our changes from both **Main** & **Newly Created Test Branch**

* We will be using **GIT Hub Desktop** https://desktop.github.com/ for this

* Open **Git Hub Desktop**, click on **Clone a Repository from the Internet** and provide URL for your repostiory	
	
	![image](https://user-images.githubusercontent.com/95003669/166857074-05712eb8-e1e6-4c1f-9adf-7b0b998452a7.png)
	

	![image](https://user-images.githubusercontent.com/95003669/166857331-89a5ba51-8fe1-443b-b6c7-7bb2ad0a4488.png)

* From **Current Branch** select **Main** and then click on **History** and then **Pic and Choose Commit** that we want to merger and **Right Click** and say **Cherry-pick <n> Commits...** option
	
	![image](https://user-images.githubusercontent.com/95003669/166859879-51e28d93-8e3c-421c-9a74-5e113dbb314d.png)

	
* Next, select branch in which we want to merge these changes, in our case its **Test_Conflict_Code** and click on **Cherry-pick <n> commits to Test_Conflict_Code** branch button
	
	Note: we are first merging change related to **Entigeration** to **Integration** from **Main** brnach to **Test_Conflict_Code**
	
	<img width="802" alt="image" src="https://user-images.githubusercontent.com/95003669/166859989-00a82e67-6046-43d3-bd5f-f8a2a20d2b77.png">

* Once done, it will show us a conflict which we need to resolve, In below screenshot it shows us both version of rows i.e. from Main branch and Test_Conflict_Code branch. I am using Sublime Text to open this file and remove row which I don't want.
	
	<img width="1017" alt="image" src="https://user-images.githubusercontent.com/95003669/166860363-4d068ff8-12cd-4ecb-9636-4b89f27b6c1e.png">

	
	<img width="1017" alt="image" src="https://user-images.githubusercontent.com/95003669/166860258-b615ea34-0b45-4c75-b70f-b31c4166ea94.png">

* Post making change, it will show **0 Conflicts** and click on **Continue Cherry-Pick**
	
	<img width="596" alt="image" src="https://user-images.githubusercontent.com/95003669/166860420-ca83ddcc-7dd6-4e76-938d-ffe874c3e17f.png">
	
* At this stage, once we compare both **Main** and **Test_Conflict_Code** branch, we will se row number 3 has same value i.e. correct spelling of **Integration**

	![image](https://user-images.githubusercontent.com/95003669/166860533-01b9ff4e-237f-4a86-a615-f53798d55490.png)

	![image](https://user-images.githubusercontent.com/95003669/166860570-a33a690c-3066-44c8-9e04-82d8391d3de9.png)
	
* At this stage, our first change which is changing from **Entigeration** to **Integration** from **Main** branch has bee successfully commited into **Test_Conflict_Code** branch.
	
* Now, lets push changes from **Test_Conflict_Code** branch to **Master** branch which is **New Cmd3 code** by creating a **Pull Request**
	
	![image](https://user-images.githubusercontent.com/95003669/166860714-0d892bd4-b6a0-4c9a-851a-8eb1aaea0f21.png)

	
* Provide necessary **Comments** and hit **Merge Pull Request** 
	
	<img width="1031" alt="image" src="https://user-images.githubusercontent.com/95003669/166860800-1a418c36-e9fd-49ac-8955-a10d155fbd7a.png">
	
	<img width="1031" alt="image" src="https://user-images.githubusercontent.com/95003669/166860829-f23d2207-354a-4dae-9ec4-c3311ea41b62.png">

* Once done we can see in both **Main** and **Test_Conflict_Code** branch we have same contents for **Test_Git.py** file which is right comment and CMD3 
	
	![image](https://user-images.githubusercontent.com/95003669/166860932-ee157003-92bc-4c3b-b0ca-d2dd95cb93a4.png)

	![image](https://user-images.githubusercontent.com/95003669/166860989-f2306fd9-174d-4cb2-af8f-5652e02413d0.png)
	
	

	

	
	

	
