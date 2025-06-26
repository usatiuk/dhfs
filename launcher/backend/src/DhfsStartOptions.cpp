//
// Created by Stepan Usatiuk on 25.06.2025.
//

#include "DhfsStartOptions.hpp"

std::vector<std::string> DhfsStartOptions::getOptions() {
    std::vector<std::string> out;
    out.emplace_back(java_home + "/bin/java");
    out.emplace_back("--enable-preview");
    out.emplace_back("-Xmx" + xmx);
    out.emplace_back("-Ddhfs.objects.writeback.limit=16777216");
    out.emplace_back("-Ddhfs.objects.lru.limit=67108864");
    out.emplace_back("--add-exports");
    out.emplace_back("java.base/sun.nio.ch=ALL-UNNAMED");
    out.emplace_back("--add-exports");
    out.emplace_back("java.base/jdk.internal.access=ALL-UNNAMED");
    out.emplace_back("--add-opens=java.base/java.nio=ALL-UNNAMED");
    // out.emplace_back("-Dquarkus.http.host=0.0.0.0");
    // out.emplace_back("-Dquarkus.log.category.\"com.usatiuk\".level=INFO");
    // out.emplace_back("-Dquarkus.log.category.\"com.usatiuk.dhfs\".level=INFO");
    out.emplace_back("-Ddhfs.fuse.root=" + mount_path);
    out.emplace_back("-Ddhfs.objects.persistence.root=" + data_path);
    out.emplace_back("-Ddhfs.webui.root=" + webui_path);
    for (auto option: extra_options) {
        out.emplace_back(option);
    }
    out.emplace_back("-jar");
    out.emplace_back(jar_path);
    return out;
}
