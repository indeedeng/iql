package com.indeed.imhotep.web;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.indeed.util.io.Files;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URL;

/**
 * @author vladimir
 */

/**
 * Returns to the user a file that contains a short Bash script that can be used to access IQL from the shell
 */
@Controller
public class BashScriptServlet {
    @RequestMapping("/iql.sh")
    @ResponseBody
    protected String doGet(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        resp.setHeader("content-disposition",  "attachment; filename=iql.sh");
        final String server = req.getServerName();
        final String protocol;
        if(server.contains("qa.indeed") || server.contains("stage.indeed") || server.contains("indeed.com")) {
            protocol = "https://";
        } else {
            protocol = "http://";
        }

        final URL scriptResourceURL = Resources.getResource("iql.sh");
        final String script = Resources.toString(scriptResourceURL, Charsets.UTF_8);
        final String serverURLVariable = "SERVER_URL=" + protocol + server + ":" + req.getServerPort() + req.getContextPath() + "/query\n";
        return  serverURLVariable + script;
    }
}
