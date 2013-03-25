package cws.core;

public interface VMListener {
    public void vmLaunched(VM vm);

    public void vmTerminated(VM vm);
}
