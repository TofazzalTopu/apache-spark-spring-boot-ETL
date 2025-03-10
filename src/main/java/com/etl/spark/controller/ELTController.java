package com.etl.spark.controller;

import com.etl.spark.service.ELTService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/elt")
public class ELTController {

    @Autowired
    private ELTService eltService;

    @GetMapping
    public String index() {
        return "Welcome to the ELT controller!";
    }
    @GetMapping("/run")
    public String runELT() {
        eltService.runELTProcess();
        return "ELT process completed!";
    }
}
