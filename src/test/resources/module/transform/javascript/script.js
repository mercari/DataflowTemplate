function myFunc1(input) {
   return input.doubleValue * 10;
}
function myFunc2(input) {
   return "Hello" + input.stringValue;
}
function myFuncWithState1(input, states) {
   var prev = states.getOrDefault("prevFloatValue", 0);
   return prev + input.floatValue;
}
function myFuncWithState2(input, states) {
   var prev = states.getOrDefault("prevDoubleValue", 0);
   if(prev > 1) {
       return "over1 " + input.stringValue;
   } else {
       return "under1 " + input.stringValue;
   }
}
function stateUpdateFunc(input, states) {
   states.put("prevFloatValue", input.floatValue);
   states.put("prevDoubleValue", input.doubleValue);
   return states;
}
