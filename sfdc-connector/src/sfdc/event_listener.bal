import ballerina/java;
import ballerina/ 'lang\.object as lang;

handle JAVA_NULL = java:createNull();

public type EventListener object {
    //should be listener, rename to just Listener

    *lang:Listener;
    private handle apiUrl = JAVA_NULL;
    private handle token = JAVA_NULL;
    private handle topic = JAVA_NULL;

    public function init(ListenerConfiguration config) {
        self.apiUrl = java:fromString(config.apiUrl);
        self.token = java:fromString(config.token);
        self.topic = java:fromString(config.topic);
    }

    public function __attach(service s, string? name) returns error? {
        return makeConnect(self.apiUrl, self.token, self.topic, s); 
        //breakdown the attach and start part
    }

    public function __detach(service s) returns error? {

    }

    public function __start() returns error? {
        return;
    }

    public function __gracefulStop() returns error? {
        return;
    }

    public function __immediateStop() returns error? {

    }
};

function makeConnect(handle apiUrl, handle bearerToken, handle topic, service serviceObject) returns error? = @java:Method {
    class: "com.ballerina.sf.BearerTokenExample"
} external;


public type ListenerConfiguration record {
    string apiUrl;
    string token;
    string topic;
};
