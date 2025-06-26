//
// Created by Stepan Usatiuk on 25.06.2025.
//

#ifndef DHFSSTARTOPTIONS_HPP
#define DHFSSTARTOPTIONS_HPP

#include <string>
#include <vector>

class DhfsStartOptions {
public:
    std::string java_home;
    std::string xmx;
    std::string mount_path;
    std::string data_path;
    std::string jar_path;
    std::string webui_path;
    std::vector<std::string> extra_options;

    std::vector<std::string> getOptions();
};


#endif //DHFSSTARTOPTIONS_HPP
