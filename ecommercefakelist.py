import random
ecommerce_fake_lst = ['ibay', 'poogle', 'metube', 'nicebook', 'rain forest inc', 'pear','solid buy','bacon city','CD','book hut',
'spoon express','sporks and more','tarjay','bed bath and books','big books','paper and more','kids reading hut','page by page','hooked on reading',
'iDea','shop books','barnes and fish','rell','bulk buy','speed readers','dead tree books','collector\'s authority','the twig book shop']

def ecommerce_randomizer_lst():
    ecommerce_random_output = (random.choice(ecommerce_fake_lst))
    print(ecommerce_random_output)
    return ecommerce_random_output

ecommerce_randomizer_lst()