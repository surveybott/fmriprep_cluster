#!/usr/bin/env python2
#
# fmriprep pbs wrapper, designed around fmriprep v1.5.0 and PBS Pro 19.1.3
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
p.add_argument('--queue',default="",help='PBS queue name')
p.add_argument('--limit',type=int,help='max number of subjects to run concurrently')
p.add_argument('--hrs-per-sub',type=int,default=24,help='number of hours to devote to each subject for walltime purposes (be liberal)',dest='hrs')
p.add_argument('--container',default='singularity',help='container executable')
p.add_argument('--container_img',default='%s/local/simg/fmriprep-latest.simg' % os.environ["HOME"],help='fmriprep container image',dest='img')
p.add_argument('--cmd_pre',default='module load singularity',help='setup code to run (inline os.system) prior to main container call. useful to setup enviorment')
args = p.parse_args()
args.out_dir = args.out_dir.replace('~',os.environ['HOME'])
args.bids_dir = args.bids_dir.replace('~',os.environ['HOME'])
args.cmd_pre = args.cmd_pre.strip()
if not args.cmd_pre.endswith(""):
    args.cmd_pre = args.cmd_pre + ";"
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

    # generate PBS job file
    logDir = os.path.join(args.out_dir,"pbs")
    if not os.path.exists(logDir):
        os.makedirs(logDir)

    print textwrap.dedent("""\
    #!/usr/bin/env python2
    #PBS -q %s
    #PBS -l select=1:ncpus=%d:mem=%dmb,walltime=%s
    #PBS -J 0-%d%s
    #PBS -N fmriprep
    #PBS -o %s/
    #PBS -e %s/

    from os import system,environ

    # setup subject array""" % (args.queue,args.ncpu,args.mem,time,n-1,limit,logDir,logDir))
    print "sub = %s" % sub
    print 'tid = int(environ["PBS_ARRAY_INDEX"])'
    print 'system("%s %s run %s %s --participant_label %%s" %% sub[tid])' % (args.cmd_pre,args.container,args.img,' '.join(fmriprep))
else:
    sys.exit('No sub- dirs found in %s' % args.bids_dir)
