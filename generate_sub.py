#!/usr/bin/env python
# coding: utf-8

# In[34]:


import nibabel as nib
from glob import glob
import os, shutil
import pandas as pd
import pickle as pkl


# In[21]:


# get sub list/data

bidsDir = '/scratch/st-tv01-1/hbn/bids'
subList = pd.read_csv('/arc/project/st-tv01-1/hbn/code/n1262.csv')
subList = list(subList.iloc[:,0])
subDir = glob(os.path.join(bidsDir,'sub-*'))
subDir_rm = [s for s in subDir if os.path.basename(s).replace('sub-','') not in subList]
subDir = [s for s in subDir if os.path.basename(s).replace('sub-','') in subList]


# In[22]:


# delete data not in sub list
# for sDir in subDir_rm:
#     shutil.rmtree(sDir)


# In[30]:


# find "done" subs (in list) with ciftis
subDone = []
for sDir in subDir:
    s = os.path.basename(sDir)
    func = glob(os.path.join(sDir,'**','*_bold.nii.gz'),recursive=True)
    #proc = glob(os.path.join(bidsDir,'derivatives',s,'**','*_space-fsLR_den-91k_bold.dtseries.nii'),recursive=True)
    proc = glob(os.path.join(bidsDir,'derivatives',s,'**','*_desc-smoothAROMAnonaggr_bold.nii.gz'),recursive=True)
    if len(proc) == len(func) and len(func) != 0:
        subDone.append(s.replace('sub-',''))
print(f'{len(subDone)}/{len(subList)} completed subs')


# In[38]:


# find to subs to run
saveFile = '/scratch/st-tv01-1/hbn/code/sub.pkl'
sub = [s for s in subList if s not in subDone]
print(f'{len(sub)} subs to run')
with open(saveFile, 'wb') as f:
    pkl.dump(sub, f)
    print(f'Saved to "{saveFile}"')

with open(saveFile.replace('.pkl','_n.txt'),'w') as f:
    f.write(f'{len(sub)}\n')

print(f'\nRun:\nqsub -J 0-{len(sub)-1} {saveFile.replace("sub.pkl","submit_pkl.pbs")}')
# In[ ]:

