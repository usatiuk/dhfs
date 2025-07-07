#import <Cocoa/Cocoa.h>
#import "utils.h"

void SetAppAsRegular(void) {
    [NSApp setActivationPolicy:NSApplicationActivationPolicyRegular];
}

void SetAppAsAccessory(void) {
    [NSApp setActivationPolicy:NSApplicationActivationPolicyAccessory];
}

