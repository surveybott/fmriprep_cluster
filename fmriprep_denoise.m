% FMRIPREP_DENOISE generates afni 3dTproject commands to regress out
% selected confounds and/or perform bandpass filtering
%
% SurveyBott, 2020, info@surveybot.com
function [nifti,cmd] = fmriprep_denoise(in,varargin)
p = inputParser;
p.addRequired('in',@isfolder);
p.addParameter('out',pwd,@ischar);
p.addParameter('ses',false);
p.addParameter('write',true,@(x) islogical(x) || isnumeric(x));
p.addParameter('overwrite',true,@(x) islogical(x) || isnumeric(x));
p.addParameter('desc','denoised',@ischar);
p.addParameter('funcStr','*_desc-smoothAROMAnonaggr_bold.nii.gz',@ischar);
p.addParameter('confoundStr','_desc-confounds_timeseries.tsv',@ischar);
p.addParameter('confounds',{'framewise_displacement','trans_x','trans_y','trans_z','rot_x','rot_y','rot_z',...
    'a_comp_cor_00','a_comp_cor_01','a_comp_cor_02','a_comp_cor_03','a_comp_cor_04','a_comp_cor_05',...
    'global_signal'},@iscellstr);
p.addParameter('bandpass',[],@(x) isnumeric(x) && numel(x)==2);
p.addParameter('tr',[],@(x) isnumeric(x) && numel(x) == 1);
p.addParameter('polort',2,@(x) isnumeric(x) && numel(x)==1);
p.addParameter('includeSub',[],@isnumeric);
p.addParameter('includeTask',{},@(x) ischar(x) || iscellstr(x));
p.parse(in,varargin{:});
inputs = p.Results;
% make out folder absolute
if ~strcmp(inputs.out(1),filesep)
   inputs.out = fullfile(pwd,inputs.out);
end
% get preprocessed niftis, add bids info
if inputs.ses
   inStr = fullfile(in,'sub-*','ses-*','func',inputs.funcStr);
else
   inStr = fullfile(in,'sub-*','func',inputs.funcStr); 
end
nifti = bids2struct(dir(inStr));
if isempty(nifti)
    error('No ''%s'' functionals found',inputs.funcStr);
else
    % remove sub and/or task
    if ~isempty(inputs.includeSub)
        idx = [nifti.sub] ~= inputs.includeSub;
        nifti(idx) = [];
        fprintf('%d runs removed whose sub didn''t match ''includeSub''\n',sum(idx));
    end
    if ~isempty(inputs.includeTask)
        idx = ~ismember({nifti.task},inputs.includeTask);
        nifti(idx) = [];
        fprintf('%d runs removed whose task didn''t match ''includeTask''\n',sum(idx));
    end
    if isempty(nifti)
       error('No runs remain');
    else
        % get matching confounds
        for i=1:numel(nifti)
            if isfield(nifti,'run') && ~isempty(nifti(i).run)
               run = sprintf('_run-%d',nifti(i).run);
            else
               run = ''; 
            end
            if isfield(nifti,'ses') && ~isempty(nifti(i).ses)
               ses = sprintf('_ses-%s',nifti(i).ses);
            else
               ses = ''; 
            end
            nifti(i).prefix = sprintf('sub-%s%s_task-%s%s',nifti(i).sub,ses,nifti(i).task,run);
            tsv = fullfile(nifti(i).path,sprintf('%s%s',nifti(i).prefix,inputs.confoundStr));
            % load and check confounds
            nifti(i).confound = [];
            if exist(tsv,'file')
                t = readtable(tsv,'FileType','text','Delimiter','\t','TreatAsEmpty','n/a');
                if isempty(inputs.confounds) || all(ismember(inputs.confounds,t.Properties.VariableNames))
                    nifti(i).confound_all = t;
                    nifti(i).confound = t(:,ismember(t.Properties.VariableNames,inputs.confounds));
                    nifti(i).confound = fillmissing(nifti(i).confound,'constant',0);
                end
            end
        end
        % remove missing confounds
        idx = arrayfun(@(x) isempty(x.confound),nifti);
        if ~isempty(inputs.confounds) && sum(idx)
           nifti(idx) = [];
           fprintf('%d runs removed without confound files and/or requested ''confounds''\n',sum(idx));
        end
        if isempty(nifti)
            error('No runs remain');
        else
            % summarize remaining
            sub = unique({nifti.sub});
            task = unique({nifti.task});
            fprintf('Creating denoising command file for %d runs from %d subs and %d tasks\n',numel(nifti),numel(sub),numel(task));
            for i=1:numel(task)
               fprintf('\t%s\t\t%d subs\n',task{i},sum(ismember({nifti.task},task{i})));
            end
            
        end
    end
    % make .1D files and 'cmd'
    if inputs.write
        if ~isfolder(inputs.out)
            mkdir(inputs.out);
        end
    end
    cmd = cell(size(nifti));
    for i=1:numel(nifti)
        out = fullfile(inputs.out,regexprep(nifti(i).name,['_' nifti(i).suffix],['_' inputs.desc '.nii.gz']));
        if exist(out,'file') && inputs.overwrite
           delete(out); % 3dTproject doesn't overwrite by default 
        end
        % setup and write confounds file 
        if ~isempty(inputs.confounds)
            ort1D = fullfile(inputs.out,sprintf('%s_desc-%s_regressors.1D',nifti(i).prefix,inputs.desc));
            ort = sprintf(' -ort ''%s''',ort1D);
            if inputs.write
                writetable(nifti(i).confound,ort1D,'FileType','text','Delimiter','\t','WriteVariableNames',false);
            end
        else
           ort = '';
        end
        % setup bandpass
        if ~isempty(inputs.bandpass)
           bandpass = sprintf(' -bandpass %f %f',inputs.bandpass(1),inputs.bandpass(2));
           if ~isempty(inputs.tr)
              bandpass = sprintf('%s -dt %f',bandpass,inputs.tr); 
           end
        else
           bandpass = ''; 
        end
        % cmd
        cmd{i} = sprintf('3dTproject -input ''%s'' -prefix ''%s'' -polort %d%s%s',...
            nifti(i).file,out,inputs.polort,ort,bandpass);
    end
    % write cmd file
    if inputs.write
       fid = fopen(fullfile(inputs.out,sprintf('cmd_desc-%s.txt',inputs.desc)),'w');
       fprintf(fid,'%s\n',cmd{:});
       fclose(fid);
    end
end
end
