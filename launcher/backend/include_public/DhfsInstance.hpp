//
// Created by stepus53 on 24.6.25.
//

#ifndef DHFSINSTANCE_HPP
#define DHFSINSTANCE_HPP

#include <jni.h>

enum class DhfsInstanceState {
    RUNNING,
    STOPPED,
};

class DhfsInstance {
public:
    DhfsInstance();

    ~DhfsInstance();

    DhfsInstanceState state();

    void start();

    void stop();

private:
    DhfsInstanceState _state = DhfsInstanceState::STOPPED;

    JavaVM* _jvm = nullptr;
    JNIEnv *_env = nullptr;
};


#endif //DHFSINSTANCE_HPP
