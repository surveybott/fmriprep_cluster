#!/usr/bin/env python2
#
# fmriprep slurm wrapper, designed around 1.5.0
#
# Jeff Eilbott, SurveyBott, 2018, info@surveybott.com
import os,argparse,sys,textwrap

# parse arguments
def is_dir(parser, arg):
    if not os.path.isdir(arg):
        parser.error('"%s" is not a directory' % arg)
    else:
        return arg

p = argparse.ArgumentParser(description='Run fmriprep pipeline in parallel by submitting an array job to the SLURM scheduler.\nJeff Eilbott, 2018, jeilbott@surveybott.com',formatter_class=argparse.ArgumentDefaultsHelpFormatter)
p.add_argument('bids_dir',help='top-level BIDS directory')
p.add_argument('out_dir',help='directory to output derivatives')
p.add_argument('--fmriprep',type=lambda x: x.split(),metavar="'--argN [argN] ...'",help='fmriprep args (surround all in one set of quotes), passed to container')
p.add_argument('--include',nargs='*',help='list of subjects to include')
p.add_argument('--exclude',nargs='*',help='list of subjects to exclude')
p.add_argument('--ncpu',type=int,default=8,help='number of cpus per subject')
p.add_argument('--mem',default=10000,type=int,metavar='MB',help='memory per subject in MB',dest='mem')
p.add_argument('--partition',default="general",help='SLURM partition')
p.add_argument('--limit',type=int,help='max number of subjects to run concurrently')
p.add_argument('--hrs-per-sub',type=int,default=24,help='number of hours to devote to each subject for walltime purposes (be liberal)',dest='hrs')
p.add_argument('--container',default='singularity',help='container executable')
p.add_argument('--container_img',default='%s/local/simg/fmriprep-latest.simg' % os.environ["HOME"],help='fmriprep container image',dest='img')
p.add_argument('--cmd_pre',default='',help='setup code to run (inline os.system) prior to main container call. useful to setup enviorment')
args = p.parse_args()
args.out_dir = args.out_dir.replace('~',os.environ['HOME'])
args.bids_dir = args.bids_dir.replace('~',os.environ['HOME'])
#print(args)

# get subject directories
sub = []
sub_dir = []
for root,dirs,files in os.walk(args.bids_dir,topdown=True):
    for dir in dirs:
        if dir.startswith("sub-"):
            # include / exclude
            if args.include is None or dir in args.include or dir.replace('sub-','') in args.include:
                if args.exclude is None or (dir not in args.exclude and dir not in args.exclude not in args.exclude):
                    sub.append(dir.replace('sub-',''))
                    sub_dir.append(os.path.join(root,dir))

    dirs[:] = [dir for dir in dirs if not dir.startswith("sub-")] # don't go walk in sub- dirs
# check sub dirs found
n = len(sub)
if n >= 1:
    # setup SLURM parameters
    time = "%d:00:00" % args.hrs

    if args.limit is not None:
        limit = "%%%d" % args.limit
    else:
        limit = ''

    # setup fmriprep parameters
    fmriprep = [args.bids_dir, args.out_dir,'participant','--nthreads',str(args.ncpu),'--mem-mb',str(args.mem)] + args.fmriprep

    # generate SLURM sbatch file
    slurmDir = os.path.join(args.out_dir,"slurm")
    if not os.path.exists(slurmDir):
        os.makedirs(slurmDir)

    print textwrap.dedent("""\
    #!/usr/bin/env python2
    #SBATCH --partition=%s
    #SBATCH --cpus-per-task=%d
    #SBATCH --mem=%dM
    #SBATCH --time=%s
    #SBATCH --array=0-%d%s
    #SBATCH --job-name=fmriprep
    #SBATCH --output=%s/fmriprep_%%A_%%a.out
    #SBATCH --error=%s/fmriprep_%%A_%%a.err

    from os import system,environ

    # setup subject array""" % (args.partition,args.ncpu,args.mem,time,n-1,limit,slurmDir,slurmDir))
    print "sub = %s" % sub
    print 'tid = int(environ["SLURM_ARRAY_TASK_ID"])'
    print 'system("%s%s run %s %s --participant_label %%s" %% sub[tid])' % (args.cmd_pre,args.container,args.img,' '.join(fmriprep))
else:
    sys.exit('No sub- dirs found in %s' % args.bids_dir)
