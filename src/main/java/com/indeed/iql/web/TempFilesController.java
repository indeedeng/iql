package com.indeed.iql.web;

import com.indeed.imhotep.utils.tempfiles.ImhotepTempFiles;
import com.indeed.imhotep.utils.tempfiles.TempFileState;
import com.indeed.util.tempfiles.IQLTempFiles;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Controller
public class TempFilesController {
    private final ImhotepTempFiles imhotepTempFiles;
    private final IQLTempFiles iqlTempFiles;

    @Autowired
    public TempFilesController(final ImhotepTempFiles imhotepTempFiles, final IQLTempFiles iqlTempFiles) {
        this.imhotepTempFiles = imhotepTempFiles;
        this.iqlTempFiles = iqlTempFiles;
    }

    @RequestMapping("/tempfiles")
    @ResponseBody
    public Map<String, List<TempFileState>> handleLastRunning() {
        final Map<String, List<TempFileState>> states = new TreeMap<>();
        states.put("imhotep", imhotepTempFiles.getAllOpenedStates());
        states.put("iql", iqlTempFiles.getAllOpenedStates());
        return states;
    }
}
