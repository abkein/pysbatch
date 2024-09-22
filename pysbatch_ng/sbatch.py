#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Copyright (c) 2023 Perevoshchikov Egor
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

# Last modified: 02-05-2024 23:40:22

import json
import re
import logging
from pathlib import Path
from typing import Dict, Union

from .config import configure
from . import constants as cs
from .utils import wexec, confdict


def run(cwd: Path, logger: logging.Logger, conf: confdict, number: Union[int, None] = None, add_conf: Union[Dict, None] = None, return_dir: bool = False) -> int:
    """Runs sbatch command via creating .job file

    Args:
        cwd (Path): current working directory
        logger (logging.Logger): Logger object
        conf (config): configuration
        number (Union[int, None], optional): Number of task. At this stage there is no jobid yet, so it used instead. Defaults to None.
        add_conf (Union[Dict, None], optional): Additional configuration, it merged to main configuration. Defaults to None.

    Raises:
        RuntimeError: Raised if sbatch command not returned jobid (or function cannot parse it from output)

    Returns:
        jobid (int): slurm's jobid
    """
    logger.debug("Preparing...")
    if add_conf is not None:
        for k, v in add_conf.items():
            conf[k] = v

    logger.debug('Configuring...')
    logger.debug("Got configuration")
    logger.debug(json.dumps(conf, indent=4))
    configure(conf, logger.getChild('configure'))
    if number:
        tdir = cwd / cs.folders.run / (cs.ps.jname + str(number))
    else:
        tdir = cwd / cs.folders.run / cs.ps.jname
    tdir.mkdir(parents=True, exist_ok=True)

    conf['jd'] = tdir.as_posix()
    conf.self_reconf()

    job_file = tdir / f"{cs.ps.jname}.job"

    with job_file.open('w') as fh:
        fh.writelines("#!/usr/bin/env bash\n")
        fh.writelines(f"#SBATCH --job-name={cs.ps.jname}\n")
        fh.writelines(f"#SBATCH --output={tdir}/{cs.ps.jname}.out\n")
        fh.writelines(f"#SBATCH --error={tdir}/{cs.ps.jname}.err\n")
        fh.writelines("#SBATCH --begin=now\n")
        if cs.ps.nnodes is not None:
            fh.writelines(f"#SBATCH --nodes={cs.ps.nnodes}\n")
        if cs.ps.ntpn is not None:
            fh.writelines(f"#SBATCH --ntasks-per-node={cs.ps.ntpn}\n")
        if cs.ps.partition is not None:
            fh.writelines(f"#SBATCH --partition={cs.ps.partition}\n")
        if cs.ps.exclude_str is not None:
            fh.writelines(f"#SBATCH --exclude={cs.ps.exclude_str}\n")
        if conf[cs.fields.args] is not None:
            fh.writelines(f"{cs.ps.pre} srun -u {conf[cs.fields.executable]} {conf[cs.fields.args]}")
        else:
            fh.writelines(f"srun -u {conf[cs.fields.executable]}")

    logger.info("Submitting task...")
    cmd = f"{cs.execs.sbatch} {job_file}"
    bout = wexec(cmd, logger.getChild('sbatch'))

    if re.match(cs.re.sbatch_jobid, bout):
        *beg, jobid_s = bout.split()
        try:
            jobid = int(jobid_s)
        except Exception as e:
            logger.error("Cannot parse sbatch jobid from:")
            logger.error(bout)
            logger.exception(e)
            raise RuntimeError("sbatch command not returned task jobid")
        print("Sbatch jobid: ", jobid)
        logger.info(f"Sbatch jobid: {jobid}")
    else:
        logger.error("Cannot parse sbatch jobid from:")
        logger.error(bout)
        raise RuntimeError("sbatch command not returned task jobid")
    return jobid


if __name__ == "__main__":
    raise NotImplementedError
