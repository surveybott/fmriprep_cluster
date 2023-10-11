import os
import re
import argparse
from glob import glob
import json
import pandas as pd
import numpy as np
from multiprocessing import Pool
import time


# find
def find_func_data(inDir, sub, suffix='*_preproc_bold.nii.gz'):
    # find all (lightly) preprocessed echo images
    echo_images_all = glob(f'{inDir}/sub-{sub}/**/sub-{sub}_{suffix}', recursive=True)
    echo_images_all = [f for f in echo_images_all if "_space-" not in f]
    image_prefix = [re.search('(.*)_echo-', os.path.basename(f)).group(1) for f in echo_images_all]
    image_prefix = set(image_prefix)
    df = []
    # loop over runs
    for prefix in image_prefix:
        sub = prefix.split('_')[0].replace('sub-','')
        ses = prefix.split('ses-')
        if len(ses) > 1:
            ses = ses[1].split('_')[0]
        else:
            ses = None
        run = 'task-'+prefix.split('_task-')[1]
        # find images matching the appropriate run prefix
        echo_images = [f for f in echo_images_all if (prefix in f)]
        echo_images.sort()
        # read echo times out of json and sort
        echo_times = [json.load(open(f.replace('.nii.gz','.json')))['EchoTime'] for f in echo_images]
        echo_times.sort()

        df.append((sub, ses, prefix, run, echo_images, echo_times))
    return pd.DataFrame(data=df, columns=['sub', 'ses', 'prefix', 'run', 'echo_images', 'echo_times'])
# find individual echo images (and get info) from fmriprep directory
def find_multiecho_data(inDir, sub):
    # find all (lightly) preprocessed echo images
    echo_images_all = glob(f'{inDir}/sub-{sub}/**/sub-{sub}_*_echo-*preproc_bold.nii.gz', recursive=True)
    echo_images_all = [f for f in echo_images_all if "_space-" not in f]
    image_prefix = [re.search('(.*)_echo-', os.path.basename(f)).group(1) for f in echo_images_all]
    image_prefix = set(image_prefix)
    df = []
    # loop over runs
    for prefix in image_prefix:
        sub = prefix.split('_')[0].replace('sub-','')
        ses = prefix.split('ses-')
        if len(ses) > 1:
            ses = ses[1].split('_')[0]
        else:
            ses = None
        run = 'task-'+prefix.split('_task-')[1]
        # find images matching the appropriate run prefix
        echo_images = [f for f in echo_images_all if (prefix in f)]
        echo_images.sort()
        # read echo times out of json and sort
        echo_times = [json.load(open(f.replace('.nii.gz','.json')))['EchoTime'] for f in echo_images]
        echo_times.sort()

        df.append((sub, ses, prefix, run, echo_images, echo_times))
    return pd.DataFrame(data=df, columns=['sub', 'ses', 'prefix', 'run', 'echo_images', 'echo_times'])


# find tedana outputs
def find_tedana_outputs(inDir, prefix, spaces=["Native", "T1w", "MNI152NLin6Asym"], suffix="optcomDenoised_bold"):
    nifti = {}
    for s in spaces:
        nifti[s] = np.nan
        file = os.path.join(inDir, f'{prefix}_space-{s}_desc-{suffix}.nii.gz')
        if os.path.isfile(file):
            nifti[s] = file
    nifti["prefix"] = prefix
    return nifti


# find individual (subject-level) transformations
def find_anat_xfm(inDir, sub, ses=None):
    files = glob(os.path.join(inDir, f'sub-{sub}', 'anat', f'sub-{sub}_from-*_to-*'))
    if not files and ses is not None:
        files = glob(os.path.join(inDir, 'sub-'+sub, 'ses-'+ses, 'anat', f'sub-{sub}_ses-{ses}_from-*_to-*'))
    xfm = {}
    for f in files:
        if 'from-T1w' in f:
            xfm[re.search('to-(.*)_mode', f).group(1)] = f
    return xfm


# find bold to T1w transformation
def find_bold_xfm(inDir, sub, ses, prefix):
    if ses is not None:
        xfm = os.path.join(inDir, f'sub-{sub}', f'ses-{ses}', 'func', f'{prefix}_from-scanner_to-T1w_mode-image_xfm.txt')
    else:
        xfm = os.path.join(inDir, f'sub-{sub}', 'func', f'{prefix}_from-scanner_to-T1w_mode-image_xfm.txt')
    if os.path.isfile(xfm):
        return xfm
    else:
        return None


# find t1w image and brain mask
def find_t1w(inDir, sub, ses=None):
    output = {}
    prefix = f'sub-{sub}'
    anat = os.path.join(inDir, f'sub-{sub}', 'anat')
    if not os.path.exists(anat):
        if ses is None or not os.path.exists(os.path.join(inDir, f'sub-{sub}', f'ses-{ses}', 'anat')):
            return None
        else:
            anat = os.path.join(inDir, f'sub-{sub}', f'ses-{ses}', 'anat')
            prefix = f'{prefix}_ses-{ses}'
    files = {'image': f'{prefix}_desc-preproc_T1w.nii.gz', 'mask': f'{prefix}_desc-brain_mask.nii.gz'}
    for key, value in files.items():
        f = os.path.join(anat, value)
        if not os.path.exists(f):
            f = None
        output[key] = f
    return output


def init_func_to_cifti_prep_wf(grayord_density, name='func_to_cifti_prep_wf'):

    from niworkflows.engine.workflows import LiterateWorkflow as Workflow
    from nipype.pipeline import engine as pe
    from nipype.interfaces import utility as niu
    from niworkflows.interfaces.fixes import FixHeaderApplyTransforms as ApplyTransforms
    #from niworkflows.interfaces.itk import MultiApplyTransforms
    from niworkflows.interfaces.nibabel import GenerateSamplingReference

    #from fmriprep.interfaces.maths import Clip

    fslr_density, mni_density = ("32k", "2") if grayord_density == "91k" else ("59k", "1")

    workflow = Workflow(name=name)
    inputnode = pe.Node(
        niu.IdentityInterface(
            fields=[
                'func_bold',
                't1w_brain',
                't1w_mask',
                'xfm_bold_to_t1w',
                'xfm_t1w_to_std'
            ]
        ),
        name='inputnode'
    )

    outputnode = pe.Node(
        niu.IdentityInterface(
            fields=['bold_t1w', 'bold_std']
        ),
        name='outputnode',
    )

    get_tpl = pe.Node(
        niu.Function(function=_get_template),
        name="get_tpl",
        run_without_submitting=True
    )
    get_tpl.inputs.space = 'MNI152NLin6Asym'
    get_tpl.inputs.resolution = mni_density

    gen_t1w_ref = pe.Node(
        GenerateSamplingReference(), name='gen_t1w_ref', mem_gb=0.3
    )  # 256x256x256 * 64 / 8 ~ 150MB

    merge_std_tfms = pe.Node(
        niu.Merge(2),
        name="mask_merge_tfms",
        run_without_submitting=True,
        mem_gb=0.1,
    )

    func_t1w_tfm = pe.Node(
        ApplyTransforms(interpolation='LanczosWindowedSinc', float=True, input_image_type=3, out_postfix=''),
        name='func_t1w_tfm', mem_gb=1
    )

    func_std_tfm = pe.Node(
        ApplyTransforms(interpolation='LanczosWindowedSinc', float=True, input_image_type=3, out_postfix=''),
        name='func_std_tfm', mem_gb=1
    )

        # fmt:off
    workflow.connect([
        (inputnode, gen_t1w_ref, [('func_bold', 'moving_image'),
                              ('t1w_brain', 'fixed_image'),
                              ('t1w_mask', 'fov_mask')]),
        (inputnode, func_t1w_tfm, [('func_bold', 'input_image'),
                                  ('xfm_bold_to_t1w','transforms')]),
        (inputnode, func_std_tfm, [('func_bold', 'input_image')]),
        (inputnode, merge_std_tfms, [('xfm_t1w_to_std','in1'),
                                    ('xfm_bold_to_t1w','in2')]),
        (merge_std_tfms, func_std_tfm, [('out', 'transforms')]),
        (get_tpl, func_std_tfm, [('out', 'reference_image')]),
        (gen_t1w_ref, func_t1w_tfm, [('out_file', 'reference_image')]),
        (func_t1w_tfm, outputnode, [('output_image', 'bold_t1w')]),
        (func_std_tfm, outputnode, [('output_image', 'bold_std')])
    ])

    return workflow

def _get_template(space, resolution=None):
    from niworkflows.utils.misc import get_template_specs
    specs = {}
    if resolution is not None:
        specs = {'resolution':resolution}
    return get_template_specs(space, specs)[0]


# run tedana_workflow
def run_tedana(prefix, echo_images, echo_times, out_dir, fittype='curvefit', tedpca='kundu', gscontrol=None):
    from tedana.workflows import tedana_workflow
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)

    tedana_workflow(
        echo_images,
        echo_times,
        out_dir=out_dir,
        prefix="%s_space-Native" % (prefix),
        fittype=fittype,
        tedpca=tedpca,
        verbose=True,
        gscontrol=gscontrol)


def run_cifti_wf(inDir, workingDir, row, density='91k'):
    from fmriprep.workflows.bold.resampling import init_bold_surf_wf, init_bold_grayords_wf
    import nipype.interfaces.io as nio
    from nipype.interfaces import utility as niu
    from nipype.pipeline import engine as pe

    wf = pe.Workflow(name=f'{row["prefix"]}_cifti_wf', base_dir=os.path.join(workingDir, "tedana_cifti_wf"))
    prep_wf = init_func_to_cifti_prep_wf(grayord_density=density)
    surf_wf = init_bold_surf_wf(surface_spaces=["fsaverage"], medial_surface_nan=True, mem_gb=2)
    cifti_wf = init_bold_grayords_wf(grayord_density=density, repetition_time=2, mem_gb=5)

    ds = pe.Node(nio.DataSink(parameterization=False), name='datasinker')
    ds.inputs.base_directory = inDir
    ds.inputs.substitutions = [("space-Native", f'space-fsLR_den-{density}')]

    prep_wf.inputs.inputnode.func_bold = row['Native']
    prep_wf.inputs.inputnode.t1w_brain = row['t1w']
    prep_wf.inputs.inputnode.t1w_mask = row['t1w_mask']
    prep_wf.inputs.inputnode.xfm_bold_to_t1w = row['xfm_bold']
    prep_wf.inputs.inputnode.xfm_t1w_to_std = row['xfm_anat']

    wf.connect(prep_wf, "outputnode.bold_t1w", surf_wf, "inputnode.source_file")
    to_list = pe.Node(niu.Merge(1), name="to_list", run_without_submitting=True, mem_gb=0.1)
    wf.connect(prep_wf, "outputnode.bold_std", to_list, "in1")
    wf.connect(to_list, "out", cifti_wf, "inputnode.bold_std")

    surf_wf.inputs.inputnode.subject_id = 'sub-' + row['sub']
    surf_wf.inputs.inputnode.subjects_dir = os.path.join(inDir, "sourcedata", "freesurfer")
    surf_wf.inputs.inputnode.t1w2fsnative_xfm = row["xfm_fsnative"]

    wf.connect(surf_wf, "outputnode.surfaces", cifti_wf, "inputnode.surf_files")

    cifti_wf.inputs.inputnode.spatial_reference = ['MNI152NLin6Asym_res-2']
    cifti_wf.inputs.inputnode.surf_refs = ["fsaverage"]
    cifti_wf.inputs.inputnode.subjects_dir = os.path.join(inDir, "sourcedata", "freesurfer")

    outfolder = f'sub-{row["sub"]}'
    if row['ses'] is not None:
        outfolder = f'{outfolder}.ses-{row["ses"]}'
    wf.connect(cifti_wf, "outputnode.cifti_bold", ds, f'{outfolder}.func')

    wf.run()


def main(inDir, workingDir, sub, cores, space='MNI152NLin6Asym', tedana=True, fittype='curvefit', tedpca='kundu', gscontrol=None, cifti=True):
    # run get ME data and run tedana in parallel
    df = find_multiecho_data(inDir, sub)

    if not df.empty:
        # add 'out_dir'
        df['out_dir'] = df['prefix'].apply(lambda x: os.path.join(inDir, "tedana", x.split('_')[0], x))
        # run tedana
        if tedana:
            data = df.loc[:, ['prefix', 'echo_images', 'echo_times', 'out_dir']].values.tolist()
            data = [d + [fittype, tedpca, gscontrol] for d in data]
            pool = Pool(cores)
            pool.starmap(run_tedana, data)
            pool.close()
        # find tedana outputs and transform to cifti
        pool = Pool(cores)
        outputs = pool.starmap(find_tedana_outputs, df.loc[:, ['out_dir', 'prefix']].values.tolist())
        pool.close()
        df = pd.merge(df, pd.DataFrame(outputs))
        if any(~df.loc[:, 'Native'].isna()):
            args = []
            for index, row in df.iterrows():
                # get t1w and std transformations
                xfm_anat = find_anat_xfm(inDir, sub, row["ses"])
                df.loc[index, "xfm_anat"] = xfm_anat[space]
                df.loc[index, "xfm_fsnative"] = xfm_anat["fsnative"]
                df.loc[index, "xfm_bold"] = find_bold_xfm(inDir, row['sub'], row['ses'], row['prefix'])
                t1w = find_t1w(inDir, row['sub'], row['ses'])
                df.loc[index, "t1w"] = t1w['image']
                df.loc[index, 't1w_mask'] = t1w['mask']
                # setup cifti pipeline calls
                required = ["Native", "xfm_bold", "xfm_anat", "xfm_fsnative", "t1w", "t1w_mask"]
                if not any(df.loc[index, required].isna()):
                    args.append((inDir, workingDir, df.loc[index, :]))
                elif cifti:
                    print(f'ERROR: f{df.loc[index,"prefix"]} missing required file(s)')
            # run cifti pipeline
            if cifti:
                    pool = Pool(cores)
                    pool.starmap(run_cifti_wf, args)
                    pool.close()
        else:
            raise Exception("No tedana outputs found")
    else:
        raise Exception('Could not find any multiecho data')
    return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='run tedana (after fmriprep) and transform outputs to standard space')
    parser.add_argument('--derivativeDir', default=None, type=str, help='fmriprep derivative directory', required=True)
    parser.add_argument('--workingDir', default=None, type=str, help='fmriprep working directory', required=True)
    parser.add_argument('--sub', default=None, type=str, help='subject name (without "sub-")', required=True)
    parser.add_argument('--cores', default=4, type=int)
    parser.add_argument('--skipTedana', default=True, action='store_false', help='don\'t run tedana')
    parser.add_argument('--skipCifti', default=True, action='store_false', help='don\'t transform tedana outputs to CIFTI')
    parser.add_argument('--space', default='MNI152NLin6Asym', type=str)
    parser.add_argument('--fittype', default='curvefit', type=str)
    parser.add_argument('--tedpca', default='kundu', type=str)
    parser.add_argument('--gscontrol', default=None, type=str)
    args = parser.parse_args()
    if args.sub is not None:
        args.sub = args.sub.replace('sub-', '')

    main(args.derivativeDir, args.workingDir, args.sub, cores=args.cores, tedana=args.skipTedana, fittype=args.fittype, tedpca=args.tedpca, gscontrol=args.gscontrol, cifti=args.skipCifti)
