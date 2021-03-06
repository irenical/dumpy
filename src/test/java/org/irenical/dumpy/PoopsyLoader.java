package org.irenical.dumpy;

import org.irenical.dumpy.api.IExtractor;
import org.irenical.dumpy.api.ILoader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class PoopsyLoader implements ILoader< Integer > {

    private final Random random = new Random();

    @Override
    public Map<? extends IExtractor.Entity<Integer>, Status> load(List<? extends IExtractor.Entity< Integer > > entities) {
//        for (IExtractor.Entity<Integer> entity : entities) {
//            System.out.println( "[ loader( " + Thread.currentThread().getId() + " ) ] value=" + entity.getValue() );
//        }
        try {
            Thread.sleep( new Random().nextInt( 100 ) > 50 ? 4500 : 2500 );
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Map<IExtractor.Entity<Integer>, Status> result = new HashMap<>();
        for (IExtractor.Entity<Integer> entity : entities) {
            result.put( entity, random.nextInt( 100 ) > 90 ? Status.ERROR : Status.SUCCESS );
        }

        return result;
    }

}
