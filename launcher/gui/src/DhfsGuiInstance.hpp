//
// Created by Stepan Usatiuk on 25.06.2025.
//

#ifndef DHFSGUIINSTANCE_HPP
#define DHFSGUIINSTANCE_HPP
#include "DhfsInstance.hpp"


class LauncherAppMainFrame;

class DhfsGuiInstance : public DhfsInstance {
public:
    DhfsGuiInstance(LauncherAppMainFrame& parent);

    void OnStateChange() override;

    void OnRead(std::string s) override;

protected:
    LauncherAppMainFrame& _parent;
};


#endif //DHFSGUIINSTANCE_HPP
