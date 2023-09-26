#!/usr/bin/env python
# coding: utf-8

from glob import glob
import os
import pandas as pd
import pickle as pkl
import pandas as pd

def main(bidsDir, derivativesDir, suffix=['_space-fsLR_den-91k_bold.dtseries.nii','_desc-smoothAROMAnonaggr_bold.nii.gz']):

    # get subjects from directories in bidsDir
    sub = [os.path.basename(g) for g in glob(os.path.join(bidsDir,'sub-*')) if os.path.isdir(g)]
    df = pd.DataFrame(sub, columns=['sub']).set_index('sub')
    for s in df.index:
        func = glob(os.path.join(bidsDir, s, '**','*_bold.nii.gz'), recursive=True)
        df.loc[s, 'complete'] = True
        df.loc[s, 'func'] = len(func)
        if len(func) != 0:
            # find derivatives (suffix inputs)
            for j, suff in enumerate(suffix):
                out = glob(os.path.join(derivativesDir, s, '**', '*' + suff), recursive=True)
                df.loc[s, f'out{j}{suff}'] = len(out)
                if len(out) != len(func):
                    df.loc[s, 'complete'] = False

    print(f'{sum(df["complete"])}/{df.shape[0]} completed subjects')

    # find subjects to run
    subRun = df.index[df['complete'] == False]
    print(subRun)

    print(f'{len(subRun)} subjects to run')
    return(df)
    # with open(saveFile, 'wb') as f:
    #     pkl.dump(sub, f)
    #     print(f'Saved to "{saveFile}"')
    #
    # with open(saveFile.replace('.pkl','_n.txt'),'w') as f:
    #     f.write(f'{len(sub)}\n')
    #
    # print(f'\nRun:\nqsub -J 0-{len(sub)-1} {saveFile.replace("sub.pkl","submit_pkl.pbs")}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='run tedana (after fmriprep) and transform outputs to standard space')
    parser.add_argument('--bidsDir', default=None, type=str, help='bids directory', required=True)
    parser.add_argument('--derivativesDir', default="derivatives", type=str, help='fmriprep output directory (rel. to bidsDir or absolute path)')
    args = parser.parse_args()

    if not os.path.isabs(args.derivativesDir):
        args.derivativesDir = os.path.join(args.bidsDir, args.derivativesDir)

    main(args.bidsDir, args.derivativesDir)
