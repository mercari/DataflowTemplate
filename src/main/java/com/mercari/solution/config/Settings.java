package com.mercari.solution.config;

public class Settings {

    private Boolean streaming;
    private AWSSettings aws;
    private BeamSQLSettings beamsql;

    public Boolean getStreaming() {
        return streaming;
    }

    public void setStreaming(Boolean streaming) {
        this.streaming = streaming;
    }

    public AWSSettings getAws() {
        return aws;
    }

    public void setAws(AWSSettings aws) {
        this.aws = aws;
    }

    public BeamSQLSettings getBeamsql() {
        return beamsql;
    }

    public void setBeamsql(BeamSQLSettings beamsql) {
        this.beamsql = beamsql;
    }

    public class AWSSettings {

        private String accessKey;
        private String secretKey;
        private String region;

        public String getAccessKey() {
            return accessKey;
        }

        public void setAccessKey(String accessKey) {
            this.accessKey = accessKey;
        }

        public String getSecretKey() {
            return secretKey;
        }

        public void setSecretKey(String secretKey) {
            this.secretKey = secretKey;
        }

        public String getRegion() {
            return region;
        }

        public void setRegion(String region) {
            this.region = region;
        }

    }

    public class BeamSQLSettings {

        private String plannerName;

        public String getPlannerName() {
            return plannerName;
        }

        public void setPlannerName(String plannerName) {
            this.plannerName = plannerName;
        }

    }

}
