//
// Created by stepus53 on 24.6.25.
//

#ifndef DHFSINSTANCE_HPP
#define DHFSINSTANCE_HPP

#include <jni.h>
#include <string>
#include <vector>

enum class DhfsInstanceState {
    RUNNING,
    STOPPED,
};

class DhfsInstance {
public:
    DhfsInstance();

    ~DhfsInstance();

    DhfsInstanceState state();

    void start(const std::string& mount_path, const std::vector<std::string>& extra_options);

    void stop();

private:
    DhfsInstanceState _state = DhfsInstanceState::STOPPED;

    JavaVM* _jvm = nullptr;
    JNIEnv* _env = nullptr;
};


#endif //DHFSINSTANCE_HPP
